#!/usr/bin/env bash
set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

res_path="${script_dir}/../results/singlewarehouse-tpcc/caracal"
script_py="${script_dir}/res-parse.py"
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
