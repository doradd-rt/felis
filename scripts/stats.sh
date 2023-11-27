#!/bin/bash

Path="/home/scofield/caracal/felis/results/cont7/noskew/nodep/caracal-pieces/"
processLatency="/home/scofield/caracal/felis/scripts/p99-latency.py"
processThroughput="/home/scofield/caracal/felis/scripts/throughput.py"

cd $Path || exit

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
