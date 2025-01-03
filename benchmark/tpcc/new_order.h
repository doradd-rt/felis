#ifndef TPCC_NEW_ORDER_H
#define TPCC_NEW_ORDER_H

#include "tpcc.h"
#include "txn_cc.h"
#include "piece_cc.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct NewOrderStruct {
  static constexpr int kNewOrderMaxItems = 15;

  uint warehouse_id;
  uint district_id;
  uint customer_id;

  uint ts_now;

  struct OrderDetail {
    uint nr_items;
    uint item_id[kNewOrderMaxItems];
    uint supplier_warehouse_id[kNewOrderMaxItems];
    uint order_quantities[kNewOrderMaxItems];

    uint unit_price[kNewOrderMaxItems];
  } detail;
};


struct NewOrderState {

  /*
  VHandle *items[15]; // read-only
  struct ItemsLookupCompletion : public TxnStateCompletion<NewOrderState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->items[id] = rows[0];
    }
  };
  NodeBitmap items_nodes;
  */

  VHandle *orderlines[15]; // insert
  struct OrderLinesInsertCompletion : public TxnStateCompletion<NewOrderState> {
    Tuple<NewOrderStruct::OrderDetail> args;
    void operator()(int id, VHandle *row) {
      state->orderlines[id] = row;
      handle(row).AppendNewVersion();

      auto &[detail] = args;
      auto amount = detail.unit_price[id] * detail.order_quantities[id];

      handle(row).WriteTryInline(
          OrderLine::Value::New(detail.item_id[id], 0, amount,
                                detail.supplier_warehouse_id[id],
                                detail.order_quantities[id]));
    }
  };
  NodeBitmap orderlines_nodes;

  VHandle *oorder; // insert
  VHandle *neworder; // insert
  VHandle *cididx; // insert

  struct OtherInsertCompletion : public TxnStateCompletion<NewOrderState> {
    OOrder::Value args;
    void operator()(int id, VHandle *row) {
      handle(row).AppendNewVersion();
      if (id == 0) {
        state->oorder = row;
        handle(row).WriteTryInline(args);
      } else if (id == 1) {
        state->neworder = row;
        handle(row).WriteTryInline(NewOrder::Value());
      } else if (id == 2) {
        state->cididx = row;
        handle(row).WriteTryInline(OOrderCIdIdx::Value());
      }
      // handle(row).AppendNewVersion(id < 2);
    }
  };
  NodeBitmap other_inserts_nodes;

  VHandle *stocks[15]; // update
  InvokeHandle<NewOrderState, unsigned int, bool, int> stock_futures[15];
  struct StocksLookupCompletion : public TxnStateCompletion<NewOrderState> {
    Tuple<int> args = Tuple<int>(-1);
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      debug(DBG_WORKLOAD "AppendNewVersion {} sid {}", (void *) rows[0], handle.serial_id());
      auto [bitmap]= args;
      if (bitmap == -1) {
        state->stocks[id] = rows[0];
        handle(rows[0]).AppendNewVersion(1);
      } else { // Bohm partitioning
        int idx = 0, oldid = id;
        do {
          idx = __builtin_ctz(bitmap);
          bitmap &= ~(1 << idx);
        } while (id-- > 0);

        state->stocks[idx] = rows[0];
        handle(rows[0]).AppendNewVersion();
      }
    }
  };
  NodeBitmap stocks_nodes;
};

class NewOrderTxn : public Txn<NewOrderState>, public NewOrderStruct {
  Client *client;
 public:
  NewOrderTxn(Client *client, uint64_t serial_id);
  NewOrderTxn(Client *client, uint64_t serial_id, char* &input);

  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final;
};

}

#endif
