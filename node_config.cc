#include <fstream>
#include <iterator>
#include <algorithm>

#include "json11/json11.hpp"
#include "node_config.h"
#include "console.h"
#include "log.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "epoch.h"
#include "promise.h"

namespace dolly {

NodeConfiguration *NodeConfiguration::instance = nullptr;

static const std::string kNodeConfiguration = "nodes.json";

static NodeConfiguration::NodePeerConfig ParseNodePeerConfig(json11::Json json, std::string name)
{
  NodeConfiguration::NodePeerConfig conf;
  auto &json_map = json.object_items().find(name)->second.object_items();
  conf.host = json_map.find("host")->second.string_value();
  conf.port = (uint16_t) json_map.find("port")->second.int_value();
  return conf;
}

static void ParseNodeConfig(util::Optional<NodeConfiguration::NodeConfig> &config, json11::Json json)
{
  config->worker_peer = ParseNodePeerConfig(json, "worker");
  config->web_conf = ParseNodePeerConfig(json, "web");
}

NodeConfiguration::NodeConfiguration()
{
  const char *strid = getenv("DOLLY_NODE_ID");

  if (!strid || (id = std::stoi(std::string(strid))) <= 0) {
    fprintf(stderr, "Must specify DOLLY_NODE_ID\n");
    std::abort();
  }

  std::ifstream fin(kNodeConfiguration);
  std::string conf_text{std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>()};
  std::string err;
  json11::Json conf_doc = json11::Json::parse(conf_text, err);

  if (!err.empty()) {
    fputs("Failed to parse node configuration", stderr);
    fputs(err.c_str(), stderr);
    std::abort();
  }

  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    int idx = std::stoi(it->first);
    all_config[idx] = NodeConfig();
    auto &config = all_config[idx];
    config->id = idx;
    ParseNodeConfig(config, it->second);
    max_node_id = std::max((int) max_node_id, idx);
  }
}

using go::TcpSocket;
using go::TcpInputChannel;
using util::Instance;

class NodeServerThreadRoutine : public go::Routine {
  TcpInputChannel *in;
 public:
  NodeServerThreadRoutine(TcpSocket *client_sock) : in(client_sock->input_channel()) {}
  virtual void Run() final {
    int max_nr_thread = dolly::Epoch::kNrThreads;
    int cur_thread = 1;
    while (true) {
      uint64_t promise_size = 0;
      in->Read(&promise_size, 8);
      PromiseRoutinePool *pool = PromiseRoutinePool::Create(promise_size);
      in->Read(pool->mem, promise_size);

      auto r = PromiseRoutine::CreateFromBufferedPool(pool);

      // Running r in a RR fashion
      go::GetSchedulerFromPool(++cur_thread)->WakeUp(
          go::Make([r]() { r->callback(r); }));
      if (cur_thread == max_nr_thread) cur_thread = 1;
    }
  }
};

class NodeServerRoutine : public go::Routine {
 public:
  virtual void Run() final {
    auto &console = util::Instance<Console>();

    auto server_sock = new TcpSocket(1024, 1024);
    auto &configuration = Instance<NodeConfiguration>();
    auto &node_conf = configuration.config();

    if (!server_sock->Bind("0.0.0.0", node_conf.worker_peer.port)) {
      std::abort();
    }
    if (!server_sock->Listen(NodeConfiguration::kMaxNrNode)) {
      std::abort();
    }
    console.UpdateServerStatus("listening");

    console.WaitForServerStatus("connecting");
    // Now if anybody else tries to connect to us, it should be in the listener
    // queue. We are safe to call connect at this point. It shouldn't lead to
    // deadlock.
    for (auto &config: configuration.all_config) {
      if (!config) continue;
      if (config->id == configuration.node_id()) continue;
      TcpSocket *remote_sock = new TcpSocket(1024, 1024);
      remote_sock->Connect(config->worker_peer.host, config->worker_peer.port);
      configuration.all_nodes[config->id] = remote_sock;
    }
    // Now we can begining to accept
    while (true) {
      TcpSocket *client_sock = server_sock->Accept();
      if (client_sock == nullptr) continue;
      logger->info("New Connection");
      go::Scheduler::Current()->WakeUp(new NodeServerThreadRoutine(client_sock));
    }
  }
};

void RunConsoleServer(std::string netmask, std::string service_port);

void NodeConfiguration::RunAllServers()
{
  RunConsoleServer("0.0.0.0", std::to_string(config().web_conf.port));
  logger->info("Starting node server with id {}", node_id());
  go::GetSchedulerFromPool(1)->WakeUp(new NodeServerRoutine());
}

void NodeConfiguration::TransportPromiseRoutine(PromiseRoutine *routine)
{
  uint64_t buffer_size = routine->TreeSize();
  uint8_t *buffer = (uint8_t *) malloc(buffer_size);
  routine->EncodeTree(buffer);
  auto sock = all_nodes[routine->node_id];
  if (!sock) {
    logger->critical("node {} does not exist!", routine->node_id);
    std::abort();
  }
  auto out = sock->output_channel();
  out->Write(&buffer_size, 8);
  out->Write(buffer, buffer_size);
}

}
