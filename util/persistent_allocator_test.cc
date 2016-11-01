//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "util/persistent_allocator.h"
#include "util/testharness.h"
#include <cstdio>

namespace rocksdb {

class PersistentAllocatorTest : public testing::Test {};

TEST_F(PersistentAllocatorTest, SimpleTest) {
  PersistentAllocator allocator;
  char *a = allocator.Allocate(8);
  fprintf(stdout, "asdf %lx\n", (uintptr_t) a);
  a[0] = '1';
  fprintf(stdout, "asdf %lx\n", (uintptr_t) a);
  char *b = allocator.Allocate(8);
  b[0] = '1';
  fprintf(stdout, "asdf %lx\n", (uintptr_t) b);
  ASSERT_EQ(8, b - a);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
