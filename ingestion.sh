#!/usr/bin/ksh

########## Set Spark version and HADOOP directory ##########
sparkVersion=$(cat /opt/mapr/spark/sparkversion)
source /opt/mapr/spark/spark-${sparkVersion}/conf/spark-env.sh
export PATH=$PATH:$SPARK_HOME/bin
echo "Spark version is ${sparkVersion}"


CONFIG_FILE=$1
FILE_DATE=$2 #AsOfDate value from the DIS job parameter[3]
CURR_DATE_TS=`date +'%Y_%m_%d_%H%M%S'`

echo "FILE_DATE=$FILE_DATE"

SPARK_ARGS=`echo $1 | tr -s '%' ' '`

spark-submit $SPARK_ARGS $2 --config_file $CONFIG_FILE --procd_dt $FILE_DATE --src_file_path $SRC_FILE_PATH  > $LOG_FILE_PATH/accounts_delta_file_load_${CURR_DATE_TS}.log 2>&1

if [ $? -ne 0 ]; then
        echo "Spark file ingestion failed!!!"
        exit 30001
fi
