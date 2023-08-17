import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.window  import Window
import sys

def main():
        date = sys.argv[1]
        depth = sys.argv[2]
        base_input_geo_path = sys.argv[3]
        base_input_event_path = sys.argv[4]
        base_output_path = sys.argv[5]

        spark = SparkSession.builder \
                        .master("local") \
                        .appName(f"geo_event_users_{date}_{depth}") \
                        .getOrCreate()

        logger= spark._jvm.org.apache.log4j.Logger
        log = logger.getLogger(__name__)

        log.info('- START RUN geo_event_users')
        log.info('-- define value and function')

        URL_HDFS = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

        # чтение csv файла geo.csv
        try:
                log.info('-- read csv file - geo')
                df_geo = spark.read.option('header', 'true') \
                        .option('delimiter', ';') \
                        .csv(f'{URL_HDFS}{base_input_geo_path}')

                log.info('-- prepare csv file - convert to radians')
                df_geo = df_geo.withColumn('lat', F.regexp_replace('lat', ',', '.')) \
                                .withColumn('lng', F.regexp_replace('lng', ',', '.')) \
                                .withColumn('lat_rad_geo', F.radians(F.col('lat').cast('double'))) \
                                .withColumn('lon_rad_geo', F.radians(F.col('lng').cast('double')))
                
                log.info('-- task done')       
        except:
                log.error('- dont read files')
        # чтение events
        try:
                log.info('-- read parquet files')
                df = spark.read.parquet(f'{URL_HDFS}{base_input_geo_path}') \
                                .filter(F.col('date') \
                                        .between(F.date_sub(F.to_date(F.lit(date),'yyyy-MM-dd'), depth), date))

                log.info('-- prepare parquet files - convert to radians')
                df = df.withColumn('lat_rad', F.radians('lat')) \
                        .withColumn('lon_rad', F.radians('lon'))
                log.info('-- task done')     
        except:
                log.error('- dont read files')
        # выполнение 1 пункта - определение города событий по координатам 
        try:
                log.info('-- prepare two df - join and calculate durations')
                df_geo_event = df.join(df_geo, how='cross') \
                        .withColumn('sin_lat', F.pow(F.sin((F.col('lat_rad_geo') - F.col('lat_rad'))/F.lit(2)), 2)) \
                        .withColumn('sin_lon', F.pow(F.sin((F.col('lon_rad_geo') - F.col('lon_rad'))/F.lit(2)), 2)) \
                        .withColumn('cos_lat', F.cos(F.col('lat_rad'))) \
                        .withColumn('cos_lat_rad', F.cos(F.col('lat_rad_geo'))) \
                        .withColumn('duration', 2*6371*F.asin(F.sqrt(F.col('sin_lat') + F.col('cos_lat')*F.col('cos_lat_rad')*F.col('sin_lon')))) \
                        .withColumn('rank', F.row_number().over(Window.partitionBy('event.message_id', 'date', 'lat_rad', 'lon_rad').orderBy(F.asc('duration')))) \
                        .where('rank = 1') \
                        .select('event', 'date', 'event_type', 'city')  

                log.info('-- save df')
                df_geo_event.write.format('parquet').partitionBy('date', 'event_type') \
                        .mode('overwrite').parquet(f'{URL_HDFS}{base_output_path}/geo_event_{date[8:10]}_{date[5:7]}_{depth}')

                log.info('-- task done') 
        except:
                log.error('- dont prepare df_geo_event')
        # выполнение 2 пункта - вычисление актуального и домашнего городов
        try:
                log.info('-- calculate actual and home cities')

                df_user_actual_city = df_geo_event.select(F.col('event.message_from').alias('user_id'), 
                                                        F.col('event.message_ts'),
                                                        F.col('city')) \
                                        .withColumn('rank', F.row_number().over(Window.partitionBy('user_id') \
                                                        .orderBy(F.desc('message_ts')))) \
                                        .where('rank = 1') \
                                        .select(F.col('user_id'),
                                                F.col('message_ts'),
                                                F.col('city').alias('act_city'))

                df_user_home_city = df_geo_event.select(F.col('event.message_from').alias('user_id'),
                                                        'date', 'city') \
                                                .withColumn('lag_city', 
                                                                F.coalesce(F.lag(F.col('city')) \
                                                                        .over(Window.partitionBy('user_id').orderBy('date')),
                                                                F.lit('Undefined'))) \
                                                .where('lag_city <> city') \
                                                .withColumn('lag_date',
                                                            F.coalesce(F.lag('date').over(Window.partitionBy(['user_id']) \
                                                                .orderBy(F.asc('date'))),
                                                            F.col('date'))) \
                                                .withColumn('diff', F.datediff(F.col('date'), F.col('lag_date'))) \
                                                .where('diff >= 27') \
                                                .withColumn('rank', F.row_number().over(Window.partitionBy(['user_id']) \
                                                                .orderBy(F.desc('date')))) \
                                                .where('rank = 1') \
                                                .select('user_id', 
                                                        F.col('lag_city').alias('home_city'))

                df_user_city = df_user_actual_city.join(df_user_home_city, on='user_id', how='full_outer') \
                                        .select('user_id',
                                                'act_city',
                                                'home_city')

                log.info('-- task done') 
        except:
                log.error('- dont calculate df_user_city')
                
        # выполнение 3 пункта - расчет travel_count, travel_array
        try:
                log.info('-- calculate travel metrics')

                df_travel = df_geo_event.select(F.col('event.message_from').alias('user_id'), 
                                                        F.col('event.message_ts'),
                                                        F.col('city')) \
                                        .withColumn('lag_city', 
                                                F.coalesce(F.lag(F.col('city')) \
                                                        .over(Window.partitionBy('user_id').orderBy('message_ts')),
                                                F.first(F.col('city')) \
                                                        .over(Window.partitionBy('user_id').orderBy('message_ts')))) \
                                        .where('city <> lag_city')

                df_travel_count =  df_travel \
                                        .groupBy('user_id') \
                                        .count() \
                                        .withColumnRenamed('count', 'travel_count') 

                df_travel_array =  df_travel.select(F.col('event.message_from').alias('user_id'), 
                                                        F.col('city')) \
                                                .groupBy('user_id') \
                                                .agg(F.collect_list('city').alias('travel_array'))

                df_user_city = df_user_city.join(df_travel_count, on = 'user_id', how = 'full_outer') \
                                        .join(df_travel_array, on = 'user_id', how = 'full_outer')
                
                log.info('-- task done') 
        except:
                log.error('- dont calculate travel metrics')

        # выполнение 4 пункта - расчет местное время
        try:
                log.info('-- calculate local time')
                df_user_city = df_user_city \
                                        .withColumn('local_time', F.from_utc_timestamp(F.col('message_ts'), F.lit('Australia/ACT'))) \
                                        .select('user_id', 
                                                'act_city',
                                                'home_city',
                                                'travel_count',
                                                'travel_array',
                                                'local_time')
                                        # города в csv файле не совпадают с городами, использующиеся в спарке при формировании utc 
                                        # поэтому в коде использовал заглушку 
                                        # (можно заменять некорректные города на ближайшие, но при появлении новых городов etl можно упасть,
                                        # возможно, необходимо как-то обойти эти ошибки, но обработчика ошибок в from_utc_timestamp не нашел)
                                        # правильный код ниже
                                        #.withColumn('local_time', F.from_utc_timestamp(F.col('message_ts'), F.concat(F.lit('Australia/'), F.regexp_replace(F.col('city'), ' ', '_')))) \
                log.info('-- task done') 
        except:
                log.error('- dont calculate local time')
        
        # сохранение витрины
        try:
                log.info('-- save parquet file')
                df_user_city.repartition(1).write \
                        .parquet(f'{URL_HDFS}{base_output_path}/geo_event_users_{date[8:10]}_{date[5:7]}_{depth}')

                log.info('-- task done') 
        except:
                log.error('- dont save parquet file')

        log.info('- END geo_event_users')
    
if __name__ == "__main__":
        main()

