// -*- mode: c++ -*-

#ifndef CHAIN_TABLE_DECL_H
#define CHAIN_TABLE_DECL_H

#include "sqltypes.h"

namespace sql {

FIELD(uint64_t, k);
KEYS(ResourceKey);

FIELD(uint64_t, v);
VALUES(ResourceValue);

FIELD(uint64_t, k);
KEYS(AccountKey);

FIELD(uint64_t, v);
VALUES(AccountValue);

}

#endif
