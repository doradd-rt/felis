#!/usr/bin/env bash

date=$(date +%d-%m)
OutputParentPath="/home/scofield/caracal/test-results-"$date"/e"
OutputDir=$OutputParentPath$EpochSize

run_cmd="~/caracal/felis/buck-out/gen/db#release -c 127.0.0.1:3148 \
  -n host1 -w ycsb -Xcpu24 -Xmem18G \
  -XYcsbReadOnly8 \
  -XEpochSize$EpochSize -XNrEpoch$NrEpoch \
  -XLogFile/home/scofield/work-backup/deterdb/scripts/zipf/ycsb_uniform_no_cont.txt \
  -XOutputDir$OutputDir"

for i in {1 2 5}; do
  for k in {100 1000 10000}; do
    EpochSize=$i*$k
    echo "$EpochSize"
    if [[ $EpochSize -le 1000 ]]; then
      NrEpoch=200000/$EpochSize
    else
      NrEpoch=100
    fi
    $run_cmd
    sleep 10
  done
done
