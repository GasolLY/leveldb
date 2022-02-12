// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  //WriteBatch::rep_的前12个字节定义为Header。
  //其中前8字节存储sequence number，后4字节存储count
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

// 迭代函数 WriteBatch::Iterate 会按照顺序将 rep_ 中存储的键值对操作放到 hander 上执行
Status WriteBatch::Iterate(Handler* handler) const {
  // 将rep_作为input，并从String转换为Slice类型
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  // 删除前8个字节的seqnum以及4字节的record count
  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    // 解析当前Key/Value的状态，取值为【kTypeValue | kTypeDeletion】
    char tag = input[0];
    // 取出状态信息后，直接删除
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          // 核心的执行步骤。将KV对的数据，直接插入到memtable
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          // 核心的执行步骤
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  // 对比 解析出的操作的数目 与 rep_记录的应有的操作数目
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}
// 因为是友元类，所以可以访问WriteBatch类的私有成员rep_
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  // 从第9个字节开始，即count开始的位置。由于count占用4字节，所以Fixed32编码 
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  // 序列号占用8个字节，所以Fixed64编码
  EncodeFixed64(&b->rep_[0], seq);
}

// 存储Key和Value的信息
void WriteBatch::Put(const Slice& key, const Slice& value) {
  // 首先，计数器+1
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // 向rep_中写入操作类型，再写入键值对
  rep_.push_back(static_cast<char>(kTypeValue));
  // PutLengthPrefixedSlice 函数会 写入字符串的长度 并 写入字符串的内容
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

// 追加删除Key信息
void WriteBatch::Delete(const Slice& key) {
  // 首先，计数器+1
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // 向rep_中写入操作类型，再写入键值对
  rep_.push_back(static_cast<char>(kTypeDeletion));
  // PutLengthPrefixedSlice 函数会 写入字符串的长度 并 写入字符串的内容
  PutLengthPrefixedSlice(&rep_, key);
}

//WriteBatch的Append操作，调用工具类的append函数，对String rep_进行操作
void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
// 继承自Handler的子类MemTableInserter，将Put和Delete转到MemTable上执行。
// MemTable即为内存数据库
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

// 根据输入参数MemTable构造MemTableInserter,
// 然后执行Iterate()函数，将writeBatch的一系列操作add到MemTable中
// 好处可能是：将WriteBatch::Iterater与MemTable解耦，因此Handler可以自行替换
Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

//赋值操作
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

// WriteBatch的Append操作，实质上是对String rep_进行操作
// 修改计数器；并将src->String rep_的data的内容append到dst->String rep_
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
