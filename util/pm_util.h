//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Arena is an implementation of Allocator class. For a request of small size,
// it allocates a block with pre-defined block size. For a request of big
// size, it uses malloc to directly get the requested size.

#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <xmmintrin.h>
#include "port/port.h"

#define mem_clwb(addr) \
  asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char*)addr));

namespace rocksdb {
namespace pmem {

const uintptr_t kFlushAlign = 64;

inline void mem_fence() {
  // TODO: should use MFENCE if flush is CLFLUSH
  _mm_sfence();
}

inline void flush_cache(const void* addr, size_t len) {
  // TODO(jhli): should use CLFLUSHOPT or CLFLUSH if CLWB is not supported
  uintptr_t addr_val = reinterpret_cast<uintptr_t>(addr);
  for (auto ptr = (addr_val & ~(kFlushAlign - 1)); ptr < addr_val + len;
       ptr += kFlushAlign) {
//    mem_clwb(reinterpret_cast<char*>(ptr));
  }
}

inline void pmem_persist(const void* addr, size_t len) {
  flush_cache(addr, len);
  mem_fence();
}

inline char* map_hint() {
  // TODO(jhli): hard code to match Mnemosyne and reuse trace processing tools.
  return reinterpret_cast<char*>(0x0000100000000000UL);
}

inline void* pmem_mmap_init(const std::string& fname, size_t len, int *fd) {
  // TODO(jhli): Only used in persistent arena now. Should extend to more be
  // general.
  if ((*fd = open(fname.c_str(), O_CREAT | O_RDWR, 0644)) < 0) {
    // TODO(jhli): Complain something
    return nullptr;
  }
  if (posix_fallocate(*fd, 0, len) != 0) {
    fprintf(stdout, "Fallocate error\n");
    return nullptr;
  }
  char* hint_addr = map_hint();
  void* base = nullptr;
  if ((base = mmap(hint_addr, len, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_POPULATE, *fd, 0)) == MAP_FAILED) {
    // TODO(jhli): Complain something
    fprintf(stdout, "Mmap error\n");
    return nullptr;
  }
  return base;
}

};  // namespace pmem
};  // namespace rocksdb
