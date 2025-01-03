#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include <initializer_list>

#include "epoch.h"
#include "varstr.h"
#include "piece.h"
#include "slice.h"
#include "contention_manager.h"
#include "opts.h"

namespace felis {

class VHandle;
class Relation;
class EpochClient;

using UpdateForKeyCallback = void (*)(VHandle *row, void *ctx);

class BaseTxn {
 protected:
  friend class EpochClient;
  friend class EpochDispatcher;

  Epoch *epoch;
  uint64_t sid;

#if defined(DISPATCHER) && defined(LATENCY)
  std::chrono::time_point<std::chrono::system_clock> init_time;
  uint32_t duration;
  //std::chrono::nanoseconds duration;
#endif

  using BrkType = std::array<mem::Brk *, NodeConfiguration::kMaxNrThreads / mem::kNrCorePerNode>;
  static BrkType g_brk;
  static int g_cur_numa_node;

 public:
  BaseTxn(uint64_t serial_id)
    : epoch(nullptr), sid(serial_id)
#if defined(DISPATCHER) && defined(LATENCY)
      , duration(0)
#endif
  {
#if defined(DISPATCHER) && defined(LATENCY)
    init_time = std::chrono::system_clock::now();
#endif
  }

  static void *operator new(size_t nr_bytes) { return g_brk[g_cur_numa_node]->Alloc(nr_bytes); }
  static void operator delete(void *ptr) {}
  static void InitBrk(long nr_epochs);

  virtual void PrepareState() {}

  virtual ~BaseTxn() {}
  virtual void Prepare() = 0;
  virtual void PrepareInsert() = 0;
  virtual void Run() = 0;

 private:
  // Entry functions for Epoch. Granola, PWV and Felis all have different
  // requirements on each phase is executed (in-order vs out-of-order).
  void Run0() {
    PieceRoutine *last = nullptr;
    if (EpochClient::g_enable_granola || EpochClient::g_enable_pwv) {
      PrepareInsert();
      Prepare();
      if (EpochClient::g_enable_pwv)
        last = root_promise()->last();
    }
    Run();

    if (!last) { // !PWV
      root_promise()->AssignSchedulingKey(serial_id());
    } else { // PWV
      uint64_t k = 0;
      for (int i = 0; i < root_promise()->nr_routines(); i++) {
        auto r = root_promise()->routine(i);
        r->sched_key = k;
        if (r == last) k = serial_id();
      }
    }
  }
  void Prepare0() {
    if (EpochClient::g_enable_granola || EpochClient::g_enable_pwv)
      return;

    Prepare();
  }
  void PrepareInsert0() {
    if (EpochClient::g_enable_granola || EpochClient::g_enable_pwv)
      return;
    PrepareInsert();
  }
 public:

  virtual BasePieceCollection *root_promise() = 0;
  virtual void ResetRoot() = 0;

  uint64_t serial_id() const { return sid; }
  uint64_t epoch_nr() const { return sid >> 32; }

  class BaseTxnRow {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
    VHandle *vhandle;
   public:
    BaseTxnRow(uint64_t sid, uint64_t epoch_nr, VHandle *vhandle)
        : sid(sid), epoch_nr(epoch_nr), vhandle(vhandle) {}

    uint64_t serial_id() const { return sid; }

    void AppendNewVersion(int ondemand_split_weight = 0);
    VarStr *ReadVarStr();
    bool WriteVarStr(VarStr *obj);
    bool Delete() { return WriteVarStr(nullptr); }
  };

  class BaseTxnHandle {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
   public:
    BaseTxnHandle(uint64_t sid, uint64_t epoch_nr) : sid(sid), epoch_nr(epoch_nr) {}
    BaseTxnHandle() {}

    uint64_t serial_id() const { return sid; }

    // C++ wrapper will name hide this.
    BaseTxnRow operator()(VHandle *vhandle) const { return BaseTxnRow(sid, epoch_nr, vhandle); }
  };

  // C++ wrapper will name hide this.
  BaseTxnHandle index_handle() { return BaseTxnHandle(sid, epoch->id()); }

  // Low level API for UpdateForKey (on demand splitting)
  int64_t UpdateForKeyAffinity(int node, VHandle *row);

  struct BaseTxnIndexOpContext {
    static constexpr size_t kMaxPackedKeys = 38;
    BaseTxnHandle handle;
    EpochObject state;

    // We can batch a lot of keys in the same context. We also should mark if
    // some keys are not used at all. Therefore, we need a bitmap.
    uint64_t keys_bitmap;
    uint64_t slices_bitmap;
    uint64_t rels_bitmap;

    uint64_t key_len[kMaxPackedKeys];
    const uint8_t *key_data[kMaxPackedKeys];
    int64_t slice_ids[kMaxPackedKeys];
    int64_t relation_ids[kMaxPackedKeys];

    template <typename Func>
    static void ForEachWithBitmap(uint64_t bitmap, Func f) {
      int max_cnt = __builtin_popcountll(bitmap);
      for (int i = 0, j = 0; i < max_cnt && i < kMaxPackedKeys; i++) {
        const uint64_t mask = (1 << i);
        if (bitmap & mask) {
          f(j, i);
          j++;
        }
      }
    }

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    // We also need to send three bitmaps.
    static constexpr size_t kHeaderSize =
        sizeof(BaseTxnHandle) + sizeof(EpochObject)
        + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);

    BaseTxnIndexOpContext(BaseTxnHandle handle, EpochObject state,
                      uint64_t keys_bitmap, VarStr **keys,
                      uint64_t slices_bitmap, int64_t *slice_ids,
                      uint64_t rels_bitmap, int64_t *rels);


    BaseTxnIndexOpContext() {}

    size_t EncodeSize() const;
    uint8_t *EncodeTo(uint8_t *buf) const;
    const uint8_t *DecodeFrom(const uint8_t *buf);
  };

  static constexpr size_t kMaxRangeScanKeys = BaseTxnIndexOpContext::kMaxPackedKeys + 1;
  using LookupRowResult = std::array<VHandle *, kMaxRangeScanKeys>;

  static LookupRowResult BaseTxnIndexOpLookup(const BaseTxnIndexOpContext &ctx, int idx);
  static VHandle *BaseTxnIndexOpInsert(const BaseTxnIndexOpContext &ctx, int idx);
};

}

#endif /* TXN_H */
