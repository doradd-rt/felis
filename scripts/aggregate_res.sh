#!/usr/bin/env bash

res_path="/home/scofield/caracal/felis/results/cont7/noskew/nodep/caracal-pieces/"
script_py="/home/scofield/caracal/felis/scripts/res-parse.py"
res_log="agg_res.txt"

if [[ -e $res_log ]]; then
  touch $res_log
fi
log_path="$(pwd)/$res_log"

cd $res_path
for res in $(ls -v); do
  if [[ -f $res ]]; then
    echo "start process $res"
    echo "$res" >> $log_path
    python $script_py $res >> $log_path
    echo "" >> $log_path
  fi
done
