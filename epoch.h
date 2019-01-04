#ifndef EPOCH_H
#define EPOCH_H

#include <cstdint>
#include <array>
#include <map>
#include "node_config.h"
#include "util.h"
#include "mem.h"
#include "completion.h"

namespace felis {

class Epoch;
class BaseTxn;
class EpochClient;

class EpochCallback {
  friend EpochClient;
  PerfLog perf;
  EpochClient *client;
  std::string label;
  std::function<void ()> continuation;
 public:
  EpochCallback(EpochClient *client) : client(client) {}
  void operator()();
};

class EpochClient {
 public:
  static EpochClient *g_workload_client;

  EpochClient() noexcept;
  virtual ~EpochClient() {}

  void Start();

  auto completion_object() { return &completion; }

  virtual unsigned int LoadPercentage() = 0;

  // This x100 will be total number of txns.
  static constexpr size_t kEpochBase = 3000;
 protected:
  friend class BaseTxn;
  friend class EpochCallback;

  void InitializeEpoch();
  void ExecuteEpoch();

  uint64_t GenerateSerialId(uint64_t sequence);

  virtual BaseTxn *RunCreateTxn(uint64_t serial_id) = 0;

 private:
  void RunTxnPromises(std::string label, std::function<void ()> continuation);

 protected:
  EpochCallback callback;
  CompletionObject<util::Ref<EpochCallback>> completion;

  util::OwnPtr<BaseTxn *[]> txns;
  unsigned long total_nr_txn;
  bool disable_load_balance;

  NodeConfiguration &conf;
};

class EpochManager {
  mem::Pool *pool;

  static EpochManager *instance;
  template <class T> friend T& util::Instance() noexcept;

  static constexpr int kMaxConcurrentEpochs = 2;
  std::array<Epoch *, kMaxConcurrentEpochs> concurrent_epochs;
  uint64_t cur_epoch_nr;

  EpochManager();
 public:
  Epoch *epoch(uint64_t epoch_nr) const;
  uint8_t *ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const;

  uint64_t current_epoch_nr() const { return cur_epoch_nr; }
  Epoch *current_epoch() const { return epoch(cur_epoch_nr); }

  void DoAdvance(EpochClient *client);
};

template <typename T>
class EpochObject {
  uint64_t epoch_nr;
  int node_id;
  uint64_t offset;

  friend class Epoch;
  EpochObject(uint64_t epoch_nr, int node_id, uint64_t offset) : epoch_nr(epoch_nr), node_id(node_id), offset(offset) {}
 public:
  EpochObject() : epoch_nr(0), node_id(0), offset(0) {}

  operator T*() const {
    return this->operator->();
  }

  T *operator->() const {
    return (T *) util::Instance<EpochManager>().ptr(epoch_nr, node_id, offset);
  }

  template <typename P>
  EpochObject<P> Convert(P *ptr) {
    uint8_t *p = (uint8_t *) ptr;
    uint8_t *self = util::Instance<EpochManager>().ptr(epoch_nr, node_id, offset);
    int64_t off = p - self;
    return EpochObject<P>(epoch_nr, node_id, offset + off);
  }

  int origin_node_id() const { return node_id; }
};

// This where we store objects across the entire cluster. Note that these
// objects are replicated but not synchronized. We need to make these objects
// perfectly partitioned.
//
// We mainly use this to store the transaction execution states, but we could
// store other types of POJOs as well.
//
// The allocator is simply an brk. Objects were replicated by replicating the
// node_id and the offset.
class EpochMemory {
 protected:
  struct {
    uint8_t *mem;
    uint64_t off;
  } brks[NodeConfiguration::kMaxNrNode];
  mem::Pool *pool;

  friend class EpochManager;
  EpochMemory(mem::Pool *pool);
  ~EpochMemory();
 public:
};

class Epoch : public EpochMemory {
 protected:
  uint64_t epoch_nr;
  EpochClient *client;
  friend class EpochManager;

  // std::array<int, NodeConfiguration::kMaxNrNode> counter;
 public:
  Epoch(uint64_t epoch_nr, EpochClient *client, mem::Pool *pool) : epoch_nr(epoch_nr), client(client), EpochMemory(pool) {}
  template <typename T>
  EpochObject<T> AllocateEpochObject(int node_id) {
    auto off = brks[node_id - 1].off;
    brks[node_id - 1].off += util::Align(sizeof(T), 8);
    return EpochObject<T>(epoch_nr, node_id, off);
  }

  template <typename T>
  EpochObject<T> AllocateEpochObjectOnCurrentNode() {
    return AllocateEpochObject<T>(util::Instance<NodeConfiguration>().node_id());
  }

  uint64_t id() const { return epoch_nr; }

  EpochClient *epoch_client() const { return client; }
};


// For scheduling transactions during execution
class EpochExecutionDispatchService : public PromiseRoutineDispatchService {
  template <typename T> friend T &util::Instance() noexcept;
  EpochExecutionDispatchService();

  using ExecutionRoutine = BasePromise::ExecutionRoutine;

  struct Entity {
    size_t dupcnt;
    BasePromise::ExecutionRoutine *last;
  };
  using Mapping = std::map<uint64_t, Entity>;
  struct CompleteCounter {
    ulong completed;
    bool force;
    CompleteCounter() : completed(0), force(false) {}
  };

  static constexpr size_t kMaxNrThreads = NodeConfiguration::kMaxNrThreads;

  std::array<Mapping, kMaxNrThreads> mappings;
  std::array<ulong, kMaxNrThreads> nr_zeros;
  std::array<CompleteCounter, kMaxNrThreads> completed_counters;
 public:
  void Dispatch(int core_id,
                BasePromise::ExecutionRoutine *exec_routine,
                go::Scheduler::Queue *q) final override;
  void Detach(int core_id,
              BasePromise::ExecutionRoutine *exec_routine) final override;
  void Reset() final override;
  void Complete(int core_id) final override;
  void PrintInfo() final override;
};

// We use thread-local brks to reduce the memory allocation cost for all
// promises within an epoch. After an epoch is done, we can reclaim all of them.
class EpochPromiseAllocationService : public PromiseAllocationService {
  template <typename T> friend T &util::Instance() noexcept;
  EpochPromiseAllocationService();

  mem::Brk *brks;
  mem::Brk *minibrks; // for mini objects
 public:
  void *Alloc(size_t size) final override;
  void Reset() final override;
};

}

#endif /* EPOCH_H */
