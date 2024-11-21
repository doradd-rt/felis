#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
res_path="${script_dir}/../results/cont0/noskew/nodep/caracal-pieces/"
processLatency="${script_dir}/p99-latency.py"
processThroughput="${script_dir}/throughput.py"

cd $res_path || exit

for subdir in $(ls -v); do # ne$(nrEpoch)
  cd "$subdir" || exit
  log="$subdir-res"
  cd ../
  touch $log
  log_path="$(pwd)/$log"
  cd "$subdir"
  for subsubdir in $(ls -v); do #ia$(interArrival)
    cd "$subsubdir" || exit
    echo "$subsubdir" >> $log_path
    for file in *; do
      if [[ -f "$file" ]]; then
        # latency
        if [[ "$file" == *latency* ]]; then
          cat "$file" | sort -n | uniq -c > "res"
          sed -i "1d" "res"
          python $processLatency "res" >> $log_path
          rm "res"
          echo "Processed $file"
        fi
        # throughput
        if [[ "$file" == *.json ]]; then
          python $processThroughput $file >> $log_path
        fi
      fi
    done
    cd ..
  done
  cd ..
done
