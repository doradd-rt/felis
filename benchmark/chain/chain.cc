#include "chain.h"
#include "index.h"
#include "txn_cc.h"
#include "pwv_graph.h"
#include "util/os.h"

namespace chain {

using namespace felis;

static constexpr int kTotal = 10;
static constexpr int kNrMSBContentionKey = 6;

class DummySliceRouter {
 public:
  static int SliceToNodeId(int16_t slice_id) { return 1; } // Always on node 1
};


// static uint64_t *g_permutation_map;

struct RMWStruct {
  uint64_t resrc_keys[Nft::kResrcPerTxn];
  uint64_t acc_keys[Nft::kAccPerTxn];
};

struct RMWState {
  VHandle *resrc_rows[Nft::kResrcPerTxn];
  InvokeHandle<RMWState> resrc_futures[Nft::kResrcPerTxn];

  struct ResrcLookupCompletion : public TxnStateCompletion<RMWState> {
    void operator()(int id, BaseTxn::LookupRowResult resrc_rows) {
      state->resrc_rows[id] = resrc_rows[0];
      //if (id < kTotal - Client::g_extra_read) {
      if (id < Nft::kResrcPerTxn) {
        //bool last = (id == kTotal - Client::g_extra_read - 1);
        bool last = (id == 1);
        handle(resrc_rows[0]).AppendNewVersion(last ? 0 : 1);
      }
    }
  };

  VHandle *acc_rows[Nft::kAccPerTxn];
  InvokeHandle<RMWState> acc_futures[Nft::kAccPerTxn];

  struct AccLookupCompletion : public TxnStateCompletion<RMWState> {
    void operator()(int id, BaseTxn::LookupRowResult acc_rows) {
      state->acc_rows[id] = acc_rows[0];
      //if (id < kTotal - Client::g_extra_read) {
      if (id < Nft::kAccPerTxn) {
        //bool last = (id == kTotal - Client::g_extra_read - 1);
        bool last = (id == 1);
        handle(acc_rows[0]).AppendNewVersion(last ? 0 : 1);
      }
    }
  };
};

template <>
RMWStruct Client::GenerateTransactionInput<RMWStruct>()
{
  RMWStruct s;
  return s;
}

template <>
RMWStruct Client::ParseTransactionInput<RMWStruct>(char* &input)
{
  RMWStruct s;
  auto txm = reinterpret_cast<const Nft::Marshalled*>(input);

  int i, j;
  for (i = 0; i < Nft::kResrcPerTxn; i++)
    s.resrc_keys[i] = txm->params[i];
  for (j = 0; j < Nft::kAccPerTxn; j++)
    s.acc_keys[j] = txm->params[j];

  input += Nft::MarshalledSize;
  return s;
}

char Client::zero_data[100];

class RMWTxn : public Txn<RMWState>, public RMWStruct {
  Client *client;

 public:
  RMWTxn(Client *client, uint64_t serial_id);
  RMWTxn(Client *client, uint64_t serial_id, char* &input);

  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
  template<typename T> static void WriteRow(TxnRow vhandle);
  template<typename T> static void ReadRow(TxnRow vhandle);

  static void WriteSpin();
};

RMWTxn::RMWTxn(Client *client, uint64_t serial_id)
    : Txn<RMWState>(serial_id),
      RMWStruct(client->GenerateTransactionInput<RMWStruct>()),
      client(client)
{}

RMWTxn::RMWTxn(Client *client, uint64_t serial_id, char* &input)
    : Txn<RMWState>(serial_id),
      RMWStruct(client->ParseTransactionInput<RMWStruct>(input)),
      client(client)
{}

void RMWTxn::Prepare()
{
  // lock_elision is only true for granola and PWV 
  if (!VHandleSyncService::g_lock_elision) {
    Resource::Key dbk_resrc[Nft::kResrcPerTxn];
    Account::Key dbk_acc[Nft::kAccPerTxn];

    for (int i = 0; i < Nft::kResrcPerTxn; i++) dbk_resrc[i].k = resrc_keys[i];
    for (int i = 0; i < Nft::kAccPerTxn; i++) dbk_acc[i].k = acc_keys[i];
    INIT_ROUTINE_BRK(8192);

    // Omit the return value because this workload is totally single node
    if (Nft::kResrcPerTxn > 0)
      TxnIndexLookup<DummySliceRouter, RMWState::ResrcLookupCompletion, void>(
        nullptr,
        KeyParam<Resource>(dbk_resrc, Nft::kResrcPerTxn));
    if (Nft::kAccPerTxn > 0)
      TxnIndexLookup<DummySliceRouter, RMWState::AccLookupCompletion, void>(
        nullptr,
        KeyParam<Account>(dbk_acc, Nft::kAccPerTxn));
  }
}

static thread_local int cnt = 0;

void RMWTxn::WriteSpin()
{
  long serv_t;
  if (cnt++ >= 624)
  {
    cnt = 0;
    serv_t = 1'000'000;
    printf("cnt is %d\n", cnt);
  } 
  else
  serv_t = 20000;
  auto time_now = time_ns();

  while (time_ns() < (time_now + serv_t))
    _mm_pause();
}

template<typename T>
void RMWTxn::WriteRow(TxnRow vhandle)
{
  auto dbv = vhandle.Read<typename T::Value>();
  dbv.v += 1;
  vhandle.Write(dbv);
}

template<typename T>
void RMWTxn::ReadRow(TxnRow vhandle)
{
  vhandle.Read<typename T::Value>();
}

void RMWTxn::Run()
{
#if 0 
  init_time = std::chrono::high_resolution_clock::now();
#endif

  if (!Options::kEnablePartition) {
    auto aff = std::numeric_limits<uint64_t>::max();
    root->AttachRoutine(
        MakeContext(), 1,
        [](const auto &ctx) {
         auto &[state, index_handle] = ctx;

          for (auto i = 0; i < Nft::kResrcPerTxn; i++)
            ReadRow<Resource>(index_handle(state->resrc_rows[i]));
          for (auto i = 0; i < Nft::kAccPerTxn; i++)
            ReadRow<Account>(index_handle(state->acc_rows[i]));

          WriteSpin();

          for (int i = 0; i < Nft::kResrcPerTxn; i++)
            WriteRow<Resource>(index_handle(state->resrc_rows[i]));
          for (int i = 0; i < Nft::kAccPerTxn; i++)
            WriteRow<Account>(index_handle(state->acc_rows[i]));

        },
        aff);

#if defined(DISPATCHER) && defined(LATENCY)
    //auto time_now = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double> log_duration = time_now - init_time;
    //std::chrono::duration<double> log_duration = time_now - exec_init_time;
      // log at precision - 100ns
    //duration = static_cast<uint32_t>(log_duration.count() * 1'000'000);
    //duration = std::chrono::duration_cast<std::chrono::nanoseconds>(time_now - init_time);
#endif
  } 
  //else if (Client::g_enable_granola || Client::g_enable_pwv) {
  //} 
}

void ChainLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<Resource, Account>();

  void *buf = alloca(512);

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
      auto handle = mgr.Get<chain::Resource>().SearchOrCreate(dbk.EncodeView(buf));
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
      auto handle = mgr.Get<chain::Account>().SearchOrCreate(dbk.EncodeView(buf));
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
  return new RMWTxn(this, serial_id);
}

BaseTxn *Client::ParseAndPopulateTxn(uint64_t serial_id, char* &input)
{
  return new RMWTxn(this, serial_id, input);
}

}
