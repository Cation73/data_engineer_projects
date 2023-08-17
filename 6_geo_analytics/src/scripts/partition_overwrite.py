import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import sys

def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    base_input_path = sys.argv[3]
    base_output_path = sys.argv[4]

    spark = SparkSession.builder \
                        .master("local") \
                        .appName(f"partition_overwrite_{date}_{depth}") \
                        .getOrCreate()

    logger= spark._jvm.org.apache.log4j.Logger
    log = logger.getLogger(__name__)

    log.info('- START RUN partition_overwrite')
    log.info('-- define value and function')
    URL_HDFS = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

    try:

        log.info('-- read parquet files')
        
        df = spark.read.parquet(f'{URL_HDFS}{base_input_path}') \
                    .filter(F.col('date') \
                            .between(F.date_sub(F.to_date(F.lit(date),'yyyy-MM-dd'), depth), date))

        log.info('-- task done')
    except:
        log.error('- dont read files')

    try:
        log.info('-- save parquet files')
        df.write.format('parquet').partitionBy('date', 'event_type') \
            .mode('append').save(f'{URL_HDFS}{base_output_path}/')

        log.info('-- task done')
    except:
        log.error('- dont read files')
    
    log.info('- END partition_overwrite')
    
if __name__ == "__main__":
        main()
