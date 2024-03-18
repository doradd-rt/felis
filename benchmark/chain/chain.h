#ifndef Chain_H
#define Chain_H

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

struct Resource {
  static uint32_t HashKey(const felis::VarStrView &k) {
    auto x = (uint8_t *) k.data();
    return *(uint32_t *) x;
  }

  static constexpr auto kTable = TableType::Resource;
  //TODO: change 1M to constant para
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 10000000, false);

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
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 289023, false);

  using IndexBackend = felis::HashtableIndex;
  using Key = sql::AccountKey;
  using Value = sql::AccountValue;
};

using RandRng = foedus::assorted::ZipfianRandom;

class Client : public felis::EpochClient {
  // Zipfian random generator
  RandRng rand;

  friend class RMWTxn;
  static char zero_data[100];
 public:
  static double g_theta;
  static size_t g_resource_table_size;
  static size_t g_account_table_size = 289023;
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

}

namespace felis {

using namespace chain;

SHARD_TABLE(Resource) { return 0; }
SHARD_TABLE(Account) { return 0; }

}

#endif
