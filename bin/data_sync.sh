#!bin/bash

pt=$1
dt="${pt:0:4}-${pt:4:2}-${pt:6:2}"

hive -e "alter table stg.stg_stocks_list add if not exists partition(pt='$pt')"
python /opt/datax/bin/datax.py -p"-Dpt='$pt'" /opt/datax/script/stg_stocks_list.json


hive -e "alter table stg.stg_stocks_quotes add if not exists partition(pt='$pt')"
python /opt/datax/bin/datax.py -p"-Dpt='$pt' -Ddt='$dt'" /opt/datax/script/stg_stocks_quotes.json
