// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

/**
 * LevelDB的内存池
 * 申请内存时，将申请到的内存块放入std::vector blocks_中，
 * 在Arena的生命周期结束后，统一释放掉所有申请到的内存
 * 
 * 每次会申请一个大的 block，默认大小为 4KB。
 * 而后申请 bytes 长度的空间时，
 * 如果当前 block 的剩余大小足够分配，则返回分配的内存地址并更新余下的起始位置和大小；
 * 否则将会直接申请新的 block。析构时会删除所有 block。
 * 
 * 当前空间不足时有一个优化，如果申请的空间大于 kBlockSize / 4 也就是 1KB 时，
 * 会直接申请对应长度的 block 返回，不更新当前剩余 block 的起始位置和大小，
 * 这样下次申请小空间时依然可以使用当前余下的空间；
 * 否则将放弃当前剩余空间，重新申请一块 4KB 的 block 再分配
*/
class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  // 直接分配内存
  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // 申请对齐的内存空间
  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  // alloc_ptr_标记1个4KB block内部分配内存的起始地址
  char* alloc_ptr_;
  //alloc_bytes_remaining_记录1个4KB block内部剩余可用的内存字节数
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  // blocks_存储多个4KB block
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  std::atomic<size_t> memory_usage_;
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
