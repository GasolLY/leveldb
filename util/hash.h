// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef STORAGE_LEVELDB_UTIL_HASH_H_
#define STORAGE_LEVELDB_UTIL_HASH_H_

#include <cstddef>
#include <cstdint>

namespace leveldb {
//Hash函数，每次按照四字节长度对参数data进行Hash
uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_HASH_H_
