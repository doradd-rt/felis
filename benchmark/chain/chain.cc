#include "chain.h"
#include "index.h"
#include "txn_cc.h"
#include "pwv_graph.h"
#include "util/os.h"

namespace chain {

using namespace felis;

class DummySliceRouter {
 public:
  static int SliceToNodeId(int16_t slice_id) { return 1; } // Always on node 1
};

struct ChainStruct {
  uint8_t num_writes;
  uint32_t gas;
  uint64_t resrc_keys[TxnType::kResrcPerTxn];
  uint64_t acc_keys[TxnType::kAccPerTxn];
};

struct ChainState {
  VHandle *resrc_rows[TxnType::kResrcPerTxn];

  struct ResrcLookupCompletion : public TxnStateCompletion<ChainState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->resrc_rows[id] = rows[0];
      bool last = (id == 1);
      handle(rows[0]).AppendNewVersion(last ? 0 : 1);
    }
  };

  VHandle *acc_rows[TxnType::kAccPerTxn];

  struct AccLookupCompletion : public TxnStateCompletion<ChainState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->acc_rows[id] = rows[0];
      bool last = (id == 1);
      handle(rows[0]).AppendNewVersion(last ? 0 : 1);
    }
  };
};

template <> ChainStruct Client::GenerateTransactionInput<ChainStruct>() {
  ChainStruct s;
  return s;
}

template <>
ChainStruct Client::ParseTransactionInput<ChainStruct>(char *&input) {
  ChainStruct s;
  auto txm = reinterpret_cast<const TxnType::Marshalled*>(input);

  int i, j;
  for (i = 0; i < txm->num_writes; i++)
    s.resrc_keys[i] = txm->params[i] - 1;
  for (j = 0; j < TxnType::kAccPerTxn; j++)
    s.acc_keys[j] = txm->params[j] - 1;

  if constexpr (std::is_same_v<TxnType, Mixed>) {
    s.gas = txm->gas;
    s.num_writes = txm->num_writes;
    /* abort_if(s.num_writes > TxnType::kResrcPerTxn, "??? num_writes {}", s.num_writes); */
    /* abort_if(s.num_writes == 0, "??? num_writes {}", s.num_writes); */
  }

  input += TxnType::MarshalledSize;
  return s;
}

class ChainTxn : public Txn<ChainState>, public ChainStruct {
  Client *client;

 public:
   ChainTxn(Client *client, uint64_t serial_id);
   ChainTxn(Client *client, uint64_t serial_id, char *&input);

   void Run() override final;
   void Prepare() override final;
   void PrepareInsert() override final {}
   template <typename T> static void WriteRow(TxnRow vhandle);
   template <typename T> static void ReadRow(TxnRow vhandle);

   static void WriteSpin(int gas);
};

ChainTxn::ChainTxn(Client *client, uint64_t serial_id)
    : Txn<ChainState>(serial_id),
      ChainStruct(client->GenerateTransactionInput<ChainStruct>()),
      client(client) {}

ChainTxn::ChainTxn(Client *client, uint64_t serial_id, char *&input)
    : Txn<ChainState>(serial_id),
      ChainStruct(client->ParseTransactionInput<ChainStruct>(input)),
      client(client) {}

void ChainTxn::Prepare() {
  if constexpr (std::is_same_v<TxnType, Mixed>) {
    Resource::Key dbk_resrc[num_writes];

    INIT_ROUTINE_BRK(8192);
    printf("num_w is %u\n", num_writes);
    for (auto i = 0; i < num_writes; i++) dbk_resrc[i].k = resrc_keys[i];

    TxnIndexLookup<DummySliceRouter, ChainState::ResrcLookupCompletion, void>(
          nullptr, KeyParam<Resource>(dbk_resrc, num_writes));
  } else {
    Resource::Key dbk_resrc[TxnType::kResrcPerTxn];
    Account::Key dbk_acc[TxnType::kAccPerTxn];

    for (int i = 0; i < TxnType::kResrcPerTxn; i++) dbk_resrc[i].k = resrc_keys[i];
    for (int i = 0; i < TxnType::kAccPerTxn; i++) dbk_acc[i].k = acc_keys[i];
    INIT_ROUTINE_BRK(8192);

    // Omit the return value because this workload is totally single node
    if (TxnType::kResrcPerTxn > 0)
      TxnIndexLookup<DummySliceRouter, ChainState::ResrcLookupCompletion, void>(
          nullptr, KeyParam<Resource>(dbk_resrc, TxnType::kResrcPerTxn));
    if (TxnType::kAccPerTxn > 0)
      TxnIndexLookup<DummySliceRouter, ChainState::AccLookupCompletion, void>(
          nullptr, KeyParam<Account>(dbk_acc, TxnType::kAccPerTxn));
  }
}

static thread_local int cnt = 0;

void ChainTxn::WriteSpin(int gas = 8) {
  long serv_t;
  if (cnt++ >= 624)
  {
    cnt = 0;
    serv_t = 1'000;
    printf("cnt is %d\n", cnt);
  } 
  else
    serv_t = 200;
  auto time_now = time_ns();

  while (time_ns() < (time_now + serv_t))
    _mm_pause();
}

template <typename T> void ChainTxn::WriteRow(TxnRow vhandle) {
  auto dbv = vhandle.Read<typename T::Value>();
  dbv.v += 1;
  vhandle.Write(dbv);
}

template <typename T> void ChainTxn::ReadRow(TxnRow vhandle) {
  vhandle.Read<typename T::Value>();
}

void ChainTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  if constexpr (std::is_same_v<TxnType, Mixed>) {
    root->AttachRoutine(
        MakeContext(num_writes, gas), 1,
        [](const auto &ctx) {
         auto &[state, index_handle, num_writes, gas] = ctx;

          for (auto i = 0; i < num_writes; i++)
            ReadRow<Resource>(index_handle(state->resrc_rows[i]));

          WriteSpin(gas);

          for (int i = 0; i < num_writes; i++)
            WriteRow<Resource>(index_handle(state->resrc_rows[i]));
        },
        aff);
  } else {
    root->AttachRoutine(
        MakeContext(), 1,
        [](const auto &ctx) {
         auto &[state, index_handle] = ctx;

          for (auto i = 0; i < TxnType::kResrcPerTxn; i++)
            ReadRow<Resource>(index_handle(state->resrc_rows[i]));
          for (auto i = 0; i < TxnType::kAccPerTxn; i++)
            ReadRow<Account>(index_handle(state->acc_rows[i]));

          WriteSpin();

          for (int i = 0; i < TxnType::kResrcPerTxn; i++)
            WriteRow<Resource>(index_handle(state->resrc_rows[i]));
          for (int i = 0; i < TxnType::kAccPerTxn; i++)
            WriteRow<Account>(index_handle(state->acc_rows[i]));

        },
        aff);
  }
#if defined(DISPATCHER) && defined(LATENCY)
    //auto time_now = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double> log_duration = time_now - init_time;
    //std::chrono::duration<double> log_duration = time_now - exec_init_time;
      // log at precision - 100ns
    //duration = static_cast<uint32_t>(log_duration.count() * 1'000'000);
    //duration = std::chrono::duration_cast<std::chrono::nanoseconds>(time_now - init_time);
#endif
}

void ChainLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<Resource, Account>();

  void *buf1 = alloca(1024);
  void *buf2 = alloca(1024);

  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("Init Resource Table t = %d\n", t);
    MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    unsigned long start = t * Client::g_resource_table_size / nr_threads;
    unsigned long end = (t + 1) * Client::g_resource_table_size / nr_threads;

    for (unsigned long i = start; i < end; i++) {
      Resource::Key dbk;
      Resource::Value dbv;
      dbk.k = i;
      dbv.v = 0;
      auto handle =
          mgr.Get<chain::Resource>().SearchOrCreate(dbk.EncodeView(buf1));
      // TODO: slice mapping table stuff?
      felis::InitVersion(handle, dbv.Encode());
    }
  }

  for (auto t = 0; t < nr_threads; t++) {
    printf("Init Account Table t = %d\n", t);
    MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    unsigned long start = t * Client::g_account_table_size / nr_threads;
    unsigned long end = (t + 1) * Client::g_account_table_size / nr_threads;

    for (unsigned long i = start; i < end; i++) {
      Account::Key dbk;
      Account::Value dbv;
      dbk.k = i;
      dbv.v = 0;
      auto handle =
          mgr.Get<chain::Account>().SearchOrCreate(dbk.EncodeView(buf2));
      // TODO: slice mapping table stuff?
      felis::InitVersion(handle, dbv.Encode());
    }
  }

  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();

  mem::ParallelPool::SetCurrentAffinity(-1);
  MasstreeIndex::ResetThreadInfo();

  done = true;

  // Generate a random permutation
#if 0
  g_permutation_map = new uint64_t[Client::g_table_size];
  for (size_t i = 0; i < Client::g_table_size; i++) {
    g_permutation_map[i] = i;
  }
  util::FastRandom perm_rand(1001);
  for (size_t i = Client::g_table_size - 1; i >= 1; i--) {
    auto j = perm_rand.next() % (i + 1);
    std::swap(g_permutation_map[j], g_permutation_map[i]);
  }
#endif
}

/* size_t Client::g_table_size = 10000000; */
double Client::g_theta = 0.00;
int Client::g_extra_read = 0;
int Client::g_contention_key = 0;
bool Client::g_dependency = false;

Client::Client() noexcept
{
  //rand.init(g_table_size, g_theta, 1238);
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  return new ChainTxn(this, serial_id);
}

BaseTxn *Client::ParseAndPopulateTxn(uint64_t serial_id, char* &input)
{
  return new ChainTxn(this, serial_id, input);
}

}
