#pragma once

#include <cstddef>
#include "util/allocator.h"
#include "util/pm_util.h"

namespace rocksdb {

class PersistentAllocator : public Allocator {
 public:
  PersistentAllocator() {
      std::string path = "./fooooo";
    basePtr = static_cast<char*>(pmem::pmem_mmap_init(path, 100 * 1024 * 1024));
    nowPtr = basePtr;
  }

  char* Allocate(size_t bytes) override {
      char* ret = nowPtr;
      nowPtr += bytes;
      return ret;
  }

  ~PersistentAllocator() {
    munmap(basePtr, 100 * 1024 * 1024);
  }

  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override {
      return Allocate(bytes);
  }

  virtual size_t BlockSize() const { return 0; }

 private:
  char* basePtr;
  char* nowPtr;
};

}  // namespace rocksdb
