#!/usr/bin/env bash

#date="09-30"
OutputParentPath="/home/scofield/caracal/test-results-10-04/e"
EpochSize="20000"
OutputDir=$OutputParentPath$EpochSize

~/caracal/felis/buck-out/gen/db#release -c 127.0.0.1:3148 \
  -n host1 -w ycsb -Xcpu24 -Xmem18G \
  -XYcsbReadOnly8 \
  -XVHandleBatchAppend -XOnDemandSplitting100000 \
  -XEpochSize$EpochSize -XNrEpoch100 \
  -XLogFile/home/scofield/work-backup/deterdb/scripts/zipf/ycsb_uniform_no_cont.txt \
  -XOutputDir$OutputDir \
  -XInterArrival"exp:10000"
