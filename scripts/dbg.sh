~/caracal/felis/buck-out/gen/db#release \
-c 127.0.0.1:3148 \
-n host1 -w ycsb -Xcpu16 -Xmem16G \
-XVHandleBatchAppend -XOnDemandSplitting100000 \
-XBinpackSplitting \
-XEpochSize20000 -XNrEpoch100 \
-XLogFile/home/scofield/work-backup/deterdb/scripts/zipf/txn_logs/ycsb_zipfian_high_cont.txt \
-XInterArrivalexp:100
