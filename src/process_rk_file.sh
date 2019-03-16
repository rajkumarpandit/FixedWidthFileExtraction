#!/bin/sh
cd /apps/treasury/bai/riskonnect/
source env/bin/activate
RK_PROCESS_FILE_CURRENT=rk_process_`date +%d_%b_%Y`.log
RK_PROCESS_FILE_PREVIOUS=rk_process_`date -d '1 day ago' +%d_%b_%Y`.log
cd /apps/treasury/bai/riskonnect/src/
if [ -f $RK_PROCESS_FILE_PREVIOUS ]
then
mv /apps/treasury/bai/riskonnect/src/$RK_PROCESS_FILE_PREVIOUS /apps/treasury/bai/riskonnect/src/log/
python readFile_Gen.py > /apps/treasury/bai/riskonnect/src/$RK_PROCESS_FILE_CURRENT
else
python readFile_Gen.py >> /apps/treasury/bai/riskonnect/src/$RK_PROCESS_FILE_CURRENT

fi
