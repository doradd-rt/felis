// -*- mode: c++ -*-

#ifndef VHANDLE_H
#define VHANDLE_H

#include <atomic>
#include "gopp/gopp.h"
#include "felis_probes.h"
#include "mem.h"
#include "sqltypes.h"
#include "shipping.h"
#include "entity.h"

namespace felis {

static const uintptr_t kPendingValue = 0xFE1FE190FFFFFFFF; // hope this pointer is weird enough

class VHandleSyncService {
 public:
  virtual void Notify(uint64_t bitmap) = 0;
  virtual bool IsPendingVal(uintptr_t val) = 0;
  virtual void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle) = 0;
  virtual void OfferData(volatile uintptr_t *addr, uintptr_t obj) = 0;
};

class BaseVHandle {
 public:
  static mem::ParallelSlabPool pool;
  static void InitPool();
  static void Quiescence() { pool.Quiescence(); }
 public:
  void Prefetch() const {}

  VHandleSyncService &sync();
};

class RowEntity;
class SliceManager;

class SortedArrayVHandle : public BaseVHandle {
  friend class RowEntity;
  friend class SliceManager;
  friend class GC;

  util::MCSSpinLock lock;
  short alloc_by_regionid;
  short this_coreid;
  unsigned int capacity;
  unsigned int size;
  // unsigned int value_mark;
  std::atomic_uint latest_version; // the latest written version's offset in *versions
  // versions: ptr to the version array.
  // [0, capacity - 1] stores version number, [capacity, 2 * capacity - 1] stores ptr to data
  uint64_t *versions;
  util::OwnPtr<RowEntity> row_entity;

 public:
  static void *operator new(size_t nr_bytes);

  static void operator delete(void *ptr) {
    SortedArrayVHandle *phandle = (SortedArrayVHandle *) ptr;
    pool.Free(ptr, phandle->this_coreid);
  }

  SortedArrayVHandle();
  SortedArrayVHandle(SortedArrayVHandle &&rhs) = delete;

  void AppendNewVersion(uint64_t sid, uint64_t epoch_nr);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *ReadExactVersion(uint64_t version_idx);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run = false);
  void WriteNewVersion(uint64_t epoch_nr, VarStr *obj);
  void GarbageCollect();
  void Prefetch() const { __builtin_prefetch(versions); }

  const size_t nr_versions() const { return size; }
  uint64_t first_version() const { return versions[0]; }
 private:
  void EnsureSpace();
  volatile uintptr_t *WithVersion(uint64_t sid, int &pos);
};

static_assert(sizeof(SortedArrayVHandle) <= 64, "SortedArrayVHandle is larger than a cache line");

#ifdef LL_REPLAY
class LinkListVHandle : public BaseVHandle {
  int this_coreid;
  std::atomic_bool lock;

  struct Entry {
    struct Entry *next;
    uint64_t version;
    uintptr_t object;
    int alloc_by_coreid;

    static mem::Pool *pools;

    static void *operator new(size_t nr_bytes) {
      return pools[mem::CurrentAllocAffinity()].Alloc();
    }

    static void operator delete(void *ptr) {
      Entry *p = (Entry *) ptr;
      pools[p->alloc_by_coreid].Free(ptr);
    }

    static void InitPools();
  };

  static_assert(sizeof(Entry) <= 32, "LinkList VHandle Entry is too large");

  Entry *head; // head is the largest!
  size_t size;

 public:
  static void *operator new(size_t nr_bytes) {
    return pools[mem::CurrentAllocAffinity()].Alloc();
  }

  static void operator delete(void *ptr) {
    auto *phandle = (LinkListVHandle *) ptr;
    pools[phandle->this_coreid].Free(ptr);
  }

  static void InitPools() {
    BaseVHandle::InitPools();
    Entry::InitPools();
  }

  LinkListVHandle();
  LinkListVHandle(LinkListVHandle &&rhs) = delete;

  bool AppendNewVersion(uint64_t sid, uint64_t epoch_nr);
  VarStr *ReadWithVersion(uint64_t sid);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
};

static_assert(sizeof(LinkListVHandle) <= 64, "LinkList Handle too large!");

#endif

#ifdef CALVIN_REPLAY

class CalvinVHandle : public BaseVHandle {
  short alloc_by_coreid;
  short this_coreid;

  std::atomic_bool lock;

  size_t capacity;
  size_t size;
  std::atomic_llong pos;
  uint64_t *accesses;
  VarStr *obj;
 public:
  static void *operator new(size_t nr_bytes) {
    return pools[mem::CurrentAllocAffinity()].Alloc();
  }

  static void operator delete(void *ptr) {
    auto *phandle = (CalvinVHandle *) ptr;
    pools[phandle->this_coreid].Free(ptr);
  }

  CalvinVHandle();
  CalvinVHandle(CalvinVHandle &&rhs) = delete;

  bool AppendNewVersion(uint64_t sid, uint64_t epoch_nr);
  bool AppendNewAccess(uint64_t sid, uint64_t epoch_nr, bool is_read = false);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *DirectRead(); // for read-only optimization
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
 private:
  void EnsureSpace();
  uint64_t WaitForTurn(uint64_t sid);
  bool PeekForTurn(uint64_t sid);
};

static_assert(sizeof(CalvinVHandle) <= 64, "Calvin Handle too large!");

#endif

// current relation implementation
#if (defined LL_REPLAY) || (defined CALVIN_REPLAY)

#ifdef LL_REPLAY
clss VHandle : public LinkListVHandle {};
#endif

#ifdef CALVIN_REPLAY
class VHandle : public CalvinVHandle {};
#endif

#else
class VHandle : public SortedArrayVHandle {};

// class VHandle : public SkipListVHandle {};
#endif

}


#endif /* VHANDLE_H */
