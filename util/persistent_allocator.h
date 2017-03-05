#pragma once

#include <cstddef>
#include <unistd.h>
#include <atomic>
#include "util/allocator.h"
#include "util/arena.h"
#include "util/pm_util.h"
#include "port/port.h"

namespace rocksdb {

class PersistentAllocator : public Allocator {
 public:
  PersistentAllocator() {
    path_ = "/mnt/pmfs/palloc_" + std::to_string(random());
    basePtr = static_cast<char*>(pmem::pmem_mmap_init(path_, 1200 * 1024 * 1024, &fd_));
    // Enforce to store nowPtr at the header of mmap file
//    nowPtr = new(basePtr) std::atomic<uintptr_t>();
     nowPtr = reinterpret_cast<uintptr_t*>(basePtr);
    basePtr += sizeof(nowPtr);
    *nowPtr = reinterpret_cast<uintptr_t>(basePtr);
    recoverPtr = nowPtr;
  }
  void Init(std::function<void(uintptr_t*)> func) {
    func(nowPtr);
  }

  void Recover(std::function<void(uintptr_t*)> func) {
    func(recoverPtr);
  }

  char* Allocate(size_t bytes) override {
      // TODO(jhli): use appropriate memory order
//      uintptr_t oldPtr = nowPtr->load();
//      while (!nowPtr->compare_exchange_weak(oldPtr, oldPtr + bytes)) {
//          port::AsmVolatilePause();
//      }
      uintptr_t oldPtr = *nowPtr;
      *nowPtr = oldPtr + bytes;
      pmem::pmem_persist(nowPtr, sizeof(nowPtr));
      return reinterpret_cast<char*>(oldPtr);
  }

  ~PersistentAllocator() {
    munmap(basePtr, 1200 * 1024 * 1024);
    close(fd_);
    unlink(path_.c_str());
  }

  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override {
      return Allocate(bytes);
  }

  inline uintptr_t AbsToRel(const void* abs) {
      if (!abs)
          return 0;
      return static_cast<uintptr_t>(reinterpret_cast<const char*>(abs) - basePtr);
  }

  inline void* RelToAbs(uintptr_t rel) {
      if (rel == 0)
          return nullptr;
      return reinterpret_cast<size_t>(rel) + basePtr;
  }

  virtual size_t BlockSize() const { return 0; }

 private:
  char* basePtr;
//  std::atomic<uintptr_t> *nowPtr;
  uintptr_t* nowPtr;
  uintptr_t* recoverPtr;
  int fd_;
  std::string path_;
};

}  // namespace rocksdb
