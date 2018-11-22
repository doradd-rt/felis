#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

struct NewOrderStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
  uint nr_items;

  ulong new_order_id;

  uint ts_now;

  uint item_id[15];
  uint supplier_warehouse_id[15];
  uint order_quantities[15];
};

template <>
NewOrderStruct Client::GenerateTransactionInput<NewOrderStruct>()
{
  NewOrderStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  s.customer_id = GetCustomerId();
  s.nr_items = RandomNumber(5, 15);
  s.new_order_id = PickNewOrderId(s.warehouse_id, s.district_id);

  for (int i = 0; i < s.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. This is our customization to TPC-C because we cannot
    // handle duplicate keys.
    //
    // In practice, this should be handle by the client application.
    for (int j = 0; j < i; j++)
      if (s.item_id[j] == id) goto again;

    s.item_id[i] = id;
    s.order_quantities[i] = RandomNumber(1, 10);
    if (nr_warehouses() == 1
        || RandomNumber(1, 100) > int(kNewOrderRemoteItem * 100)) {
      s.supplier_warehouse_id[i] = s.warehouse_id;
    } else {
      s.supplier_warehouse_id[i] =
          RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    }
  }
  s.ts_now = GetCurrentTime();
  return s;
}

using namespace felis;

struct NewOrderState : public BaseTxnState {
  struct {
    VHandle *district;
    VHandle *stocks[15];
  } rows;
};

class NewOrderTxn : public Txn<NewOrderState>, public NewOrderStruct, public Util {
  Client *client;
 public:
  NewOrderTxn(Client *client);
  void Run() override final;
};

NewOrderTxn::NewOrderTxn(Client *client)
    : client(client),
      NewOrderStruct(client->GenerateTransactionInput<NewOrderStruct>())
{
  INIT_ROUTINE_BRK(4096);

  PromiseProc _;
  auto district_key = District::Key::New(warehouse_id, district_id);

  int node = warehouse_to_node_id(warehouse_id);
  int lookup_node = node;
  if (warehouse_id == 5) {
    // puts("Offloading!");
    // lookup_node = 1;
  }

  _ >> TxnLookup<District>(lookup_node, district_key)
    >> TxnSetupVersion(
        node,
        [](const auto &ctx, auto *handle) {
          ctx.template _<0>()->rows.district = handle;
        });

  for (auto i = 0; i < nr_items; i++) {
    auto stock_key = Stock::Key::New(supplier_warehouse_id[i], item_id[i]);
    node = warehouse_to_node_id(supplier_warehouse_id[i]);
    _ >> TxnLookup<Stock>(node, stock_key)
      >> TxnSetupVersion(
          node,
          [](const auto &ctx, auto *handle) {
            uint i = ctx.template _<3>();
            ctx.template _<0>()->rows.stocks[i] = handle;
          },
          i);
  }
}

void NewOrderTxn::Run()
{
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::NewOrder), Client *>::Construct(tpcc::Client * client)
{
  return new tpcc::NewOrderTxn(client);
}

}
