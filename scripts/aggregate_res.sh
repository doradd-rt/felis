#!/usr/bin/env bash
set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

#res_path="/home/scofield/caracal/felis/results/cont7/noskew/nodep/caracal-pieces/"
res_path="/home/scofield/caracal/felis/results/singlewarehouse-tpcc/caracal"
script_py="/home/scofield/caracal/felis/scripts/res-parse.py"
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
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
