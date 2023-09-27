#include <unistd.h>
#include <cstdio>
#include <string_view>
#include <string>
#include <fcntl.h>
#include <sys/stat.h>

#include "module.h"
#include "node_config.h"
#include "tcp_node.h"
#include "console.h"
#include "log.h"
#include "epoch.h"
#include "opts.h"

void show_usage(const char *progname)
{
  printf("Usage: %s -w workload -n node_name -c controller_ip -p cpu_count -s cpu_core_shift -m\n\n", progname);
  puts("\t-w\tworkload name");
  puts("\t-n\tnode name");
  puts("\t-c\tcontroller IP address");

  puts("\nSee opts.h for extented options.");

  std::exit(-1);
}

namespace felis {

void ParseControllerAddress(std::string arg);

}

using namespace felis;

int main(int argc, char *argv[])
{
  int opt;
  std::string workload_name;
  std::string node_name;

  while ((opt = getopt(argc, argv, "w:n:c:X:")) != -1) {
    switch (opt) {
      case 'w':
        workload_name = std::string(optarg);
        break;
      case 'n':
        node_name = std::string(optarg);
        break;
      case 'c':
        ParseControllerAddress(std::string(optarg));
        break;
      case 'X':
        if (!Options::ParseExtentedOptions(std::string(optarg))) {
          fprintf(stderr, "Ignoring extended argument %s\n", optarg);
          std::exit(-1);
        }
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }


  NodeConfiguration::g_nr_threads = Options::kCpu.ToInt("4");
  NodeConfiguration::g_data_migration = Options::kDataMigration;
  if (Options::kEpochSize)
    EpochClient::g_txn_per_epoch = Options::kEpochSize.ToInt();

  Module<CoreModule>::ShowAllModules();
  Module<WorkloadModule>::ShowAllModules();
  puts("\n");

  if (node_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  auto &console = util::Instance<Console>();
  console.set_server_node_name(node_name);

  Module<CoreModule>::InitRequiredModules();

  util::InstanceInit<NodeConfiguration>();
  util::Instance<NodeConfiguration>().SetupNodeName(node_name);
  util::InstanceInit<TcpNodeTransport>();

  // init tables from the workload module
  Module<WorkloadModule>::InitModule(workload_name);

  // parsing txn from external logs instead of in-mem generation
  int fd = open(Options::kLogFile.Get().c_str(), O_RDONLY);
  if (fd == -1) 
  {
    logger->info("File not existed\n");
    exit(1);
  }

  struct stat sb;
  fstat(fd, &sb);
  void* ret = reinterpret_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ,
    MAP_PRIVATE | MAP_POPULATE, fd, 0));

  char* content = reinterpret_cast<char*>(ret);
  uint32_t count = *(reinterpret_cast<uint32_t*>(content));
  printf("log count is %u\n", count);
  content += sizeof(uint32_t);
  char* read_head = content;

  auto client = EpochClient::g_workload_client;
  //logger->info("Generating Benchmarks...");
  //client->GenerateBenchmarks();
  
#ifdef DISPATCHER
  logger->info("dispatcher streaming fashion\n");
  client->InitializeDispatcher(read_head, count);
#else
  logger->info("Populating txns from logs...");
  client->PopulateTxnsFromLogs(read_head, count);

  console.UpdateServerStatus(Console::ServerStatus::Listening);
  logger->info("Ready. Waiting for run command from the controller.");
  console.WaitForServerStatus(felis::Console::ServerStatus::Running);

  abort_if(EpochClient::g_workload_client == nullptr,
           "Workload Module did not setup the EpochClient properly");

  printf("\n");
  logger->info("Starting workload");
  client->Start();

  console.WaitForServerStatus(Console::ServerStatus::Exiting);
  go::WaitThreadPool();
#endif
  return 0;
}
