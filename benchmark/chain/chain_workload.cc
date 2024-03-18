#include "chain.h"
#include "module.h"
#include "opts.h"

namespace felis {

class ChainModule : public Module<WorkloadModule> {
 public:
  ChainModule() {
    info = {
      .name = "chain",
      .description = "Chain (Single Node Only)",
    };
  }
  void Init() override final {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    if (Options::kChainContentionKey) {
      chain::Client::g_contention_key = Options::kChainContentionKey.ToInt();
    }
    if (Options::kChainSkewFactor) {
      chain::Client::g_theta = 0.01 * Options::kChainSkewFactor.ToInt();
    }
    if (Options::kChainReadOnly)
      chain::Client::g_extra_read = Options::kChainReadOnly.ToInt();

    chain::Client::g_dependency = Options::kChainDependency;

    auto loader = new chain::ChainLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    EpochClient::g_workload_client = new chain::Client();
  }
};

static ChainModule chain_module;

}
