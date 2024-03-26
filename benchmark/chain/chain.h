#ifndef CHAIN_H
#define CHAIN_H

#include "table_decl.h"
#include "epoch.h"
#include "slice.h"
#include "index.h"

#include "zipfian_random.h"

namespace chain {

enum class TableType : int {
  ChainBase = 200,
  Resource,
  Account
};

class Mixed {
public:
  static constexpr uint8_t kTotal  = 38;
  static constexpr uint8_t kResrcPerTxn = 38;
  static constexpr uint8_t kAccPerTxn = 0;
  static constexpr uint64_t NUM_RESRC  = 41640;
  static constexpr uint64_t MAX_LENGTH = 38;  // write len
  static constexpr uint64_t MAX_GAS    = 2270;  // run time
  static constexpr uint16_t MarshalledSize = 512;
  static constexpr long SERV_TIME = 240'000; // should be unused
  
  struct __attribute__((packed)) Marshalled
  {
    uint8_t  num_writes;
    uint32_t gas;   
    uint32_t params[Mixed::kTotal];
    uint64_t cown_ptrs[Mixed::kTotal];
    uint8_t  pad[51];
  };
  static_assert(sizeof(Mixed::Marshalled) == Mixed::MarshalledSize);
};

class Nft {
public:
  static constexpr uint8_t  kTotal   = 2;
  static constexpr uint8_t  kResrcPerTxn = 1;
  static constexpr uint8_t  kAccPerTxn = 1;
  static constexpr uint64_t NUM_SENDER = 10783;
  static constexpr uint64_t NUM_RESRC  = 844;
  static constexpr uint8_t  MarshalledSize = 64;
  static constexpr long SERV_TIME = 240'000;
  
  struct __attribute__((packed)) Marshalled
  {
    uint32_t params[Nft::kTotal]; // resource and user
    uint64_t cown_ptrs[Nft::kTotal];
    /* uint8_t  pad[40]; */
    uint8_t num_writes;
    uint32_t gas;
    uint8_t pad[35];
  };
  static_assert(sizeof(Nft::Marshalled) == Nft::MarshalledSize);
};

class P2p {
public:
  static constexpr uint8_t  kTotal   = 2;
  static constexpr uint8_t  kResrcPerTxn = 0;
  static constexpr uint8_t  kAccPerTxn = 2;
  static constexpr uint64_t NUM_RESRC  = 0;
  static constexpr uint64_t NUM_SENDER = 51317;
  static constexpr uint64_t NUM_RECVER = 43456;
  static constexpr uint8_t  MarshalledSize = 64;
  static constexpr long SERV_TIME = 334'000;
  
  struct __attribute__((packed)) Marshalled
  {
    uint32_t params[P2p::kTotal]; // sender and receiver
    uint64_t cown_ptrs[P2p::kTotal];
    uint8_t num_writes;
    uint32_t gas;
    uint8_t pad[35];
  };
  static_assert(sizeof(P2p::Marshalled) == P2p::MarshalledSize);
};

class Dex { // Uniswap
public:
  static constexpr uint8_t  kTotal  = 1;
  static constexpr uint8_t  kResrcPerTxn = 1;
  static constexpr uint8_t  kAccPerTxn = 0;
  static constexpr uint8_t  MarshalledSize = 64;
  static constexpr uint64_t  NUM_RESRC = 330; // NUM_BUR
  static constexpr long SERV_TIME = 240'000;
  
  struct __attribute__((packed)) Marshalled
  {
    uint32_t params[Dex::kTotal]; // sender and receiver
    uint64_t cown_ptrs[Dex::kTotal];
    /* uint8_t pad[52]; */
    uint8_t num_writes;
    uint32_t gas;
    uint8_t pad[47];
  };
  static_assert(sizeof(Dex::Marshalled) == Dex::MarshalledSize);

  /* static constexpr uint64_t NUM_AVG   = 267; */
  /* static constexpr uint64_t NUM_BUR   = 330; */
};

using RandRng = foedus::assorted::ZipfianRandom;

using TxnType = Nft;

class Client : public felis::EpochClient {
  // Zipfian random generator
  RandRng rand;

  friend class ChainTxn;

public:
  static double g_theta;
  static const size_t g_resource_table_size = TxnType::NUM_RESRC;
  static const size_t g_account_table_size = 289023;
  static int g_extra_read;
  static int g_contention_key;
  static bool g_dependency;

  Client() noexcept;
  unsigned int LoadPercentage() final override { return 100; }
  felis::BaseTxn *CreateTxn(uint64_t serial_id) final override;
  felis::BaseTxn *ParseAndPopulateTxn(uint64_t serial_id, char* &input) final override;

  template <typename T> T GenerateTransactionInput();
  template <typename T> T ParseTransactionInput(char* &input);
};

class ChainLoader : public go::Routine {
  std::atomic_bool done = false;
 public:
  ChainLoader() {}
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
};

struct Resource {
  static uint32_t HashKey(const felis::VarStrView &k) {
    auto x = (uint8_t *) k.data();
    return *(uint32_t *) x;
  }

  static constexpr auto kTable = TableType::Resource;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, Client::g_resource_table_size, false);

  using IndexBackend = felis::HashtableIndex;
  using Key = sql::ResourceKey;
  using Value = sql::ResourceValue;
};

struct Account {
  static uint32_t HashKey(const felis::VarStrView &k) {
    auto x = (uint8_t *) k.data();
    return *(uint32_t *) x;
  }

  static constexpr auto kTable = TableType::Account;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, Client::g_account_table_size, false);

  using IndexBackend = felis::HashtableIndex;
  using Key = sql::AccountKey;
  using Value = sql::AccountValue;
};

}

namespace felis {

using namespace chain;

SHARD_TABLE(Resource) { return 0; }
SHARD_TABLE(Account) { return 0; }

}

#endif
