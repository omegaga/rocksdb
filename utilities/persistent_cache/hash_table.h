//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#ifndef ROCKSDB_LITE

#include <assert.h>
#include <list>
#include <vector>

#ifdef OS_LINUX
#include <sys/mman.h>
#endif

#include "include/rocksdb/env.h"
#include "util/mutexlock.h"
#include "util/persistent_allocator.h"

namespace rocksdb {

// HashTable<T, Hash, Equal>
//
// Traditional implementation of hash table with synchronization built on top
// don't perform very well in multi-core scenarios. This is an implementation
// designed for multi-core scenarios with high lock contention.
//
//                         |<-------- alpha ------------->|
//               Buckets   Collision list
//          ---- +----+    +---+---+--- ...... ---+---+---+
//         /     |    |--->|   |   |              |   |   |
//        /      +----+    +---+---+--- ...... ---+---+---+
//       /       |    |
// Locks/        +----+
// +--+/         .    .
// |  |          .    .
// +--+          .    .
// |  |          .    .
// +--+          .    .
// |  |          .    .
// +--+          .    .
//     \         +----+
//      \        |    |
//       \       +----+
//        \      |    |
//         \---- +----+
//
// The lock contention is spread over an array of locks. This helps improve
// concurrent access. The spine is designed for a certain capacity and load
// factor. When the capacity planning is done correctly we can expect
// O(load_factor = 1) insert, access and remove time.
//
// Micro benchmark on debug build gives about .5 Million/sec rate of insert,
// erase and lookup in parallel (total of about 1.5 Million ops/sec). If the
// blocks were of 4K, the hash table can support  a virtual throughput of
// 6 GB/s.
//
// T      Object type (contains both key and value)
// Hash   Function that returns an hash from type T
// Equal  Returns if two objects are equal
//        (We need explicit equal for pointer type)
//
template <class T, class Hash, class Equal>
class HashTable {
 public:
  explicit HashTable(const size_t capacity = 1024 * 1024,
                     const float load_factor = 2.0, const uint32_t nlocks = 256)
      : nbuckets_(
            static_cast<uint32_t>(load_factor ? capacity / load_factor : 0)),
        nlocks_(nlocks) {
    // pre-conditions
    assert(capacity);
    assert(load_factor);
    assert(nbuckets_);
    assert(nlocks_);

    buckets_.reset(new Bucket[nbuckets_]);
#ifdef OS_LINUX
    mlock(buckets_.get(), nbuckets_ * sizeof(Bucket));
#endif

    // initialize locks
    locks_.reset(new port::RWMutex[nlocks_]);
#ifdef OS_LINUX
    mlock(locks_.get(), nlocks_ * sizeof(port::RWMutex));
#endif

    // post-conditions
    assert(buckets_);
    assert(locks_);
    allocator_ = new PersistentAllocator();
  }

  virtual ~HashTable() { AssertEmptyBuckets(); delete allocator_; }

  //
  // Insert given record to hash table
  //
  bool Insert(const T& t) {
    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    WriteLock _(&locks_[lock_idx]);
    auto& bucket = buckets_[bucket_idx];
    return Insert(&bucket, t);
  }

  // Lookup hash table
  //
  // Please note that read lock should be held by the caller. This is because
  // the caller owns the data, and should hold the read lock as long as he
  // operates on the data.
  bool Find(const T& t, T* ret, port::RWMutex** ret_lock) {
    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    port::RWMutex& lock = locks_[lock_idx];
    lock.ReadLock();

    auto& bucket = buckets_[bucket_idx];
    if (Find(&bucket, t, ret)) {
      *ret_lock = &lock;
      return true;
    }

    lock.ReadUnlock();
    return false;
  }

  //
  // Erase a given key from the hash table
  //
  bool Erase(const T& t, T* ret) {
    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    WriteLock _(&locks_[lock_idx]);

    auto& bucket = buckets_[bucket_idx];
    return Erase(&bucket, t, ret);
  }

  // Fetch the mutex associated with a key
  // This call is used to hold the lock for a given data for extended period of
  // time.
  port::RWMutex* GetMutex(const T& t) {
    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    return &locks_[lock_idx];
  }

  void Clear(void (*fn)(T)) {
    for (uint32_t i = 0; i < nbuckets_; ++i) {
      const uint32_t lock_idx = i % nlocks_;
      WriteLock _(&locks_[lock_idx]);
      for (auto it = buckets_[i].list_; it != nullptr; it = it->next) {
        (*fn)(it->t);
      }
//      buckets_[i].list_.clear();
    }
  }

 protected:
  // Models bucket of keys that hash to the same bucket number
  struct Node {
    const T t;
    Node* next;
    Node* prev;
    Node(const T& t_) : t(t_), next(nullptr), prev(nullptr) {}
  };
  struct Bucket {
//    std::list<T> list_;
    Node* list_;
    Node* tail_;
    Bucket() : list_(nullptr), tail_(nullptr) {}
  };

  // Substitute for std::find with custom comparator operator
  Node* Find(Node* list, const T& t) {
    for (auto it = list; it != nullptr; it = it->next) {
      if (Equal()(it->t, t)) {
        return it;
      }
    }
    return nullptr;
  }

  bool Insert(Bucket* bucket, const T& t) {
    // Check if the key already exists
    auto it = Find(bucket->list_, t);
    if (it != nullptr) {
      return false;
    }

    // insert to bucket
//    bucket->list_.push_back(t);
    auto newNode = new Node(t);
    if (bucket->tail_ == nullptr) {
      // First node
      bucket->tail_ = newNode;
      bucket->list_ = newNode;
    } else {
      bucket->tail_->next = newNode;
      newNode->prev = bucket->tail_;
      bucket->tail_ = newNode;
    }
    return true;
  }

  bool Find(Bucket* bucket, const T& t, T* ret) {
    auto it = Find(bucket->list_, t);
    if (it != nullptr) {
      if (ret) {
        *ret = it->t;
      }
      return true;
    }
    return false;
  }

  bool Erase(Bucket* bucket, const T& t, T* ret) {
    auto it = Find(bucket->list_, t);
    if (it != nullptr) {
      if (ret) {
        *ret = it->t;
      }

//      bucket->list_.erase(it);
      if (it->prev != nullptr) {
        it->prev->next = it->next;
      } else {
        assert(it == bucket->list_);
        bucket->list_ = it->next;
      }
      if (it->next != nullptr) {
        it->next->prev = it->prev;
      } else {
        assert(it == bucket->tail_);
        bucket->tail_ = it->prev;
      }
      // delete it;

      return true;
    }
    return false;
  }

  // assert that all buckets are empty
  void AssertEmptyBuckets() {
#ifndef NDEBUG
    for (size_t i = 0; i < nbuckets_; ++i) {
      WriteLock _(&locks_[i % nlocks_]);
//      assert(buckets_[i].list_.empty());
    }
#endif
  }

  const uint32_t nbuckets_;                 // No. of buckets in the spine
  std::unique_ptr<Bucket[]> buckets_;       // Spine of the hash buckets
  const uint32_t nlocks_;                   // No. of locks
  std::unique_ptr<port::RWMutex[]> locks_;  // Granular locks
  PersistentAllocator* allocator_;
};

}  // namespace rocksdb

#endif
