

0 6 * * 2-6 bash /home/hadoop/Work/Quantitative-trading-of-stocks/data_sync.sh $(date -d yesterday +\%Y\%m\%d000000) >> /home/hadoop/Work/Quantitative-trading-of-stocks/log/data_sync_$(date +\%Y\%m\%d).log 2>&1
0 7 * * 2-6 bash /home/hadoop/Work/Quantitative-trading-of-stocks/data_process.sh $(date -d yesterday +\%Y\%m\%d000000) >> /home/hadoop/Work/Quantitative-trading-of-stocks/log/data_process_$(date +\%Y\%m\%d).log 2>&1


