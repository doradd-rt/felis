-c 127.0.0.1:3148 \
-n host1 -w ycsb -Xcpu16 -Xmem16G \
-XYcsbReadOnly8 \
-XVHandleBatchAppend -XOnDemandSplitting100000 \
-XEpochSize20000 -XNrEpoch100 \
-XLogFile/home/scofield/work-backup/deterdb/scripts/zipf/txn_logs/ycsb_uniform_no_cont.txt \
-XInterArrivalexp:100