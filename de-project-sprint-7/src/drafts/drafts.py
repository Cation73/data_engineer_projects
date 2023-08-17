import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window  import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
                        .master("local") \
                        .appName("Learning DataFrames") \
                        .getOrCreate()

1
df_geo = spark.read.option('header', 'true') \
                    .option('delimiter', ';') \
                .csv('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/cation73/data/geo/geo.csv')
df_geo = df_geo.withColumn('lat', F.regexp_replace('lat', ',', '.')) \
                .withColumn('lng', F.regexp_replace('lng', ',', '.')) \
                .withColumn('lat_rad_geo', F.radians(F.col('lat').cast('double'))) \
                .withColumn('lon_rad_geo', F.radians(F.col('lng').cast('double')))

df = spark.read.parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/cation73/data/geo/events')
df = df.withColumn('lat_rad', F.radians('lat')) \
        .withColumn('lon_rad', F.radians('lon'))

df_geo_event = df.join(df_geo, how='cross') \
            .withColumn('sin_lat', F.pow(F.sin((F.col('lat_rad_geo') - F.col('lat_rad'))/F.lit(2)), 2)) \
            .withColumn('sin_lon', F.pow(F.sin((F.col('lon_rad_geo') - F.col('lon_rad'))/F.lit(2)), 2)) \
            .withColumn('cos_lat', F.cos(F.col('lat_rad'))) \
            .withColumn('cos_lat_rad', F.cos(F.col('lat_rad_geo'))) \
            .withColumn('duration', 2*6371*F.asin(F.sqrt(F.col('sin_lat') + F.col('cos_lat')*F.col('cos_lat_rad')*F.col('sin_lon')))) \
            .withColumn('rank', F.row_number().over(Window.partitionBy('date', 'lat_rad', 'lon_rad').orderBy(F.asc('duration')))) \
            .where('rank = 1') \
            .select('event', 'date', 'event_type', 'city', 'id')

df_geo_event.select(F.col('event.message_from').alias('user_id'), 
                    F.col('event.message_ts'),
                    F.col('city')) \
            .withColumn('max_message_ts', F.max(F.col('message_ts')).over(Window.partitionBy('user_id'))) \
            .withColumn('min_message_ts', F.min(F.col('message_ts')).over(Window.partitionBy('user_id'))) \
            .withColumn('lag_city', 
                        F.coalesce(F.lag(F.col('city')).over(Window.partitionBy('user_id').orderBy('message_ts')),
                                  F.first(F.col('city')) \
                                      .over(Window.partitionBy('user_id').orderBy('message_ts')))) \
            .show(40)


df_city_from = df.selectExpr('event.message_from as user','lat_rad', 'lon_rad', 'date')
df_city_to = df.selectExpr('event.message_to as user','lat_rad', 'lon_rad', 'date')

df_left = df_city_from.union(df_city_to)

window = Window().partitionBy('user').orderBy('date')

df = df_city_from.union(df_city_to) \
                .select(F.col('user'), 
                        F.col('date'),
                        F.last(F.col('lat_rad'),
                               ignorenulls = True) \
                .over(window).alias('lat_to'),
                        F.last(F.col('lon_rad'),
                               ignorenulls = True) \
                        .over(window).alias('lon_to'))


df_left.join(df, how='anti', on='user').show(40)

.withColumn('sin_lat', F.pow(F.sin((F.col('lat_to') - F.col('lat_rad'))/F.lit(2)), 2)) \
.withColumn('sin_lon', F.pow(F.sin((F.col('lon_to') - F.col('lon_rad'))/F.lit(2)), 2)) \
.withColumn('cos_lat', F.cos(F.col('lat_rad'))) \
.withColumn('cos_lat_to', F.cos(F.col('lat_to'))) \
.withColumn('duration', 2*6371*F.asin(F.sqrt(F.col('sin_lat') + F.col('cos_lat')*F.col('cos_lat_rad')*F.col('sin_lon')))) \
.filter(F.col('dif')<=1)


def input_paths(date,depth):
        list_path = []

        for idx in range(depth):
            dtr = str((datetime.strptime(date, '%Y-%m-%d') - timedelta(days=idx)).strftime('%Y-%m-%d'))
            list_path.append(f'{URL_HDFS}{base_input_path}/date={dtr}')
    
        return list_path

def input_paths(date,depth):
                list_path = []

                for idx in range(depth):
                        dtr = str((datetime.strptime(date, '%Y-%m-%d') - timedelta(days=idx)).strftime('%Y-%m-%d'))
                        list_path.append(f'{URL_HDFS}{base_input_event_path}/date={dtr}')
        
                return list_path


df_travel_count = df_geo_event.select(F.col('event.message_from').alias('user_id'), 
                                                        F.col('event.message_ts'),
                                                        F.col('city')) \
                                        .withColumn('lag_city', 
                                                F.coalesce(F.lag(F.col('city')) \
                                                        .over(Window.partitionBy('user_id').orderBy('message_ts')),
                                                F.first(F.col('city')) \
                                                        .over(Window.partitionBy('user_id').orderBy('message_ts')))) \
                                        .where('city <> lag_city') \
                                        .groupBy('user_id') \
                                        .count() \
                                        .withColumnRenamed('count', 'travel_count') 

                1df_travel_array = df_geo_event.select(F.col('event.message_from').alias('user_id'), 
                                                        F.col('city')) \
                                                .groupBy('user_id') \
                                                .agg(F.collect_list('city').alias('travel_array'))

# cross join и формирование предварительного паркета
        try:
            log.info('-- calculate prefinal parquet')
            df = df_users.join(df_users \
                                .select(F.col('user_id').alias('user_id_to'),
                                        F.col('lat_rad').alias('lat_rad_to'),
                                        F.col('lon_rad').alias('lon_rad_to'),
                                        F.col('date').alias('date_to')),
                                how='cross') \
                .where('date = date_to') \
                .where('user_id <> user_id_to') \
                .withColumn('sin_lat', F.pow(F.sin((F.col('lat_rad_to') - F.col('lat_rad'))/F.lit(2)), 2)) \
                .withColumn('sin_lon', F.pow(F.sin((F.col('lon_rad_to') - F.col('lon_rad'))/F.lit(2)), 2)) \
                .withColumn('cos_lat', F.cos(F.col('lat_rad'))) \
                .withColumn('cos_lat_rad', F.cos(F.col('lat_rad_to'))) \
                .withColumn('duration', 2*6371*F.asin(F.sqrt(F.col('sin_lat') + F.col('cos_lat')*F.col('cos_lat_rad')*F.col('sin_lon')))) \
                .where('duration <= 1') \
                .withColumn('rank', F.row_number().over(Window().partitionBy('user_id', 'user_id_to').orderBy('duration'))) \
                .where('rank = 1') \
                .withColumn('processed_dttm', F.current_timestamp()) \
                .withColumn('local_time', F.from_utc_timestamp(F.col('message_ts'), F.lit('Australia/ACT'))) \
                .select(F.col('user_id').alias('user_left'),
                        F.col('user_id_to').alias('user_right'),
                        F.col('processed_dttm'),
                        F.col('id').alias('zone_id'),
                        F.col('local_time'))
        
        except:
            log.error('- dont calculate prefinal parquet')
                
        # исключение тех пар, у которых были сообщения между друг другом и тех, кто не находится в одном сообществе
        try:
            log.info('-- calculate couples users')

            df_couple_users = df_geo_event.select(F.concat(F.col('event.message_from'), 
                                                F.lit('_'),
                                                F.col('event.message_to')).alias('concat_users'))
            log.info('-- join couples users and prefinal')
            df = df.join(df_couple_users, on = 'concat_users', how = 'leftanti')

            log.info('-- calculate general channels')
            
            df_general_channels = df_geo_event \
                                            .where('event_type = "subscription"') \
                                            .select(F.col('event.subscription_channel').alias('channel_id'),
                                                    F.col('event.user').alias('user_id'))

            log.info('-- join general channels and prefinal')

            df = df.join(df_general_channels \
                                .select(F.col('user_id').alias('user_left'),
                                    F.col('channel_id').alias('channel_left')), on = 'user_left', how = 'left') \
                    .join(df_general_channels \
                                .select(F.col('user_id').alias('user_right'),
                                    F.col('channel_id').alias('channel_right')), on = 'user_right', how = 'left')

            log.info('-- filter general channels')
            result = df \
                        .where('channel_right = channel_left') \
                        .select('user_left', 'user_right',
                                'processed_dttm', 'zone_id', 'local_time')
        except:
            log.error('- dont calculate final parquet')

try:
        df_general_channels = df_geo_event \
                                            .where('event_type = "subscription"') \
                                            .select(F.col('event.subscription_channel').alias('channel_id'),
                                                    F.col('event.user').alias('user_id'))

        df_join_channels = df_general_channels.select(F.col('channel_id'),
                                                        F.col('user_id').alias('user_from')) \
                                                .join(df_general_channels \
                                                        .select(F.col('channel_id'),
                                                                F.col('user_id').alias('user_to')), 
                                                        on = 'channel_id', how = 'cross')
        
        df_couple_users = df_geo_event.select(F.col('event.message_from').alias('user_from'),
                                                F.col('event.message_to').alias('user_to'))

        df_1 = df_join_channels.join(df_couple_users, 
                                on = (df_join_channels['user_from'] == df_couple_users['user_from']) & (df_join_channels['user_to'] == df_couple_users['user_to']),
                                how = 'leftanti') \
                                .join(df_couple_users, 
                                on = (df_join_channels['user_from'] == df_couple_users['user_to']) & (df_join_channels['user_to'] == df_couple_users['user_from']),
                                how = 'leftanti')
                
                
         cond = [((df1.col1 == df2.col2) &\
         (df1.col3 == df2.col4))]       
                F.concat(F.col('event.message_from'), 
                                                F.lit('_'),
                                                F.col('event.message_to')).alias('concat_users'))

df_geo_event = df_geo_event \
                            .withColumn('month', F.trunc(F.col('date'), 'month')) \
                            .withColumn('week', F.trunc(F.col('date'), 'week')) \
                            .withColumn('is_message', F.when(df_geo_event['event_type'] == 'message', 1) \
                                                       .otherwise(0)) \
                            .withColumn('is_reaction', F.when(df_geo_event['event_type'] == 'reaction', 1) \
                                                       .otherwise(0)) \
                            .withColumn('is_subscription', F.when(df_geo_event['event_type'] == 'subscription', 1) \
                                                       .otherwise(0))
                            .groupBy(['month', 'week', 'id']) \
                            .agg(F.sum('is_message').alias('week_message'),
                                 F.sum('is_reaction').alias('week_reaction'),
                                 F.sum('is_subscription').alias('week_subscription'))


# формирование метрик
        def calculate_metrics(df, event_type, type_date, group_column = ['month', 'week', 'id']):
            df = df \
                .where(f"event_type = '{event_type}'") \
                .groupBy(group_column) \
                .count() \
                .withColumnRenamed('count', f'{type_date}_{event_type}')
            
            return df
        # для недель
        try:
            log.info('-- calculate week metrics')
            df_geo_event = df_geo_event \
                            .withColumn('month', F.trunc(F.col('date'), 'month')) \
                            .withColumn('week', F.trunc(F.col('date'), 'week'))
            df_week_metrics = calculate_metrics(df_geo_event, 'message', 'week') \
                                .join(calculate_metrics(df_geo_event, 'reaction', 'week'), 
                                        on = ['month', 'week', 'id'],
                                        how = 'full') \
                                .join(calculate_metrics(df_geo_event, 'subscription', 'week'),
                                        on = ['month', 'week', 'id'],
                                        how = 'full') \
                                .join(df_geo_event.select('month',
                                                        'week',
                                                        'id',
                                                        'event_type',
                                                        'date',
                                                        F.col('event.message_from').alias('user_id')) \
                                                .where('event_type = "message"') \
                                                .withColumn('rank', F.row_number().over(Window.partitionBy('user_id') \
                                                    .orderBy(F.asc('date')))) \
                                                .where('rank = 1') \
                                                .groupBy('month', 'week', 'id') \
                                                .count() \
                                                .withColumnRenamed('count', 'week_user'),
                                        on = ['month', 'week', 'id'],
                                        how = 'full')
            
        except:
            log.error('- dont calculate week metrics')
            
        # для месяцев
        try:
            log.info('-- calculate month metrics')
            df_month_metrics = calculate_metrics(df_geo_event, 'message', 'month', ['month', 'id']) \
                                .join(calculate_metrics(df_geo_event, 'reaction', 'month', ['month', 'id']), 
                                        on = ['month', 'id'],
                                        how = 'full') \
                                .join(calculate_metrics(df_geo_event, 'subscription', 'month', ['month', 'id']),
                                        on = ['month', 'id'],
                                        how = 'full') \
                                .join(df_geo_event.select('month',
                                                        'id',
                                                        'event_type',
                                                        'date',
                                                        F.col('event.message_from').alias('user_id')) \
                                                .where('event_type = "message"') \
                                                .withColumn('rank', F.row_number().over(Window.partitionBy('user_id') \
                                                                                            .orderBy(F.asc('date')))) \
                                                .where('rank = 1') \
                                                .groupBy('month', 'id') \
                                                .count() \
                                                .withColumnRenamed('count', 'month_user'),
                                        on = ['month', 'id'],
                                        how = 'full')   
            df_user_home_city \
            .withColumn('lag_city', 
                            F.coalesce(F.lag(F.col('city')) \
                                            .over(Window.partitionBy('user_id').orderBy('date')),
                                       F.lit('Undefined'))) \
            .where('lag_city <> city') \
            .withColumn('lag_date', F.lag('date').over(Window.partitionBy(['user_id']) \
                                    .orderBy(F.asc('date')))) \
            .withColumn('diff', F.datediff(F.col('date'), F.col('lag_date'))) \
            .where('diff >= 2') \
            .withColumn('')
            .show(40)
df_user_home_city = df_geo_event.select(F.col('event.message_from').alias('user_id'),
                                                        'date', 'city') \
                                        .withColumn('rank', F.row_number().over(Window.partitionBy(['user_id',  'city']) \
                                                        .orderBy(F.asc('date')))) \
                                        .withColumn('min_date', F.min('date').over(Window.partitionBy(['user_id', 'city']) \
                                                      .orderBy(F.asc('rank')))) \
                                        .withColumn('max_date', F.max('date').over(Window.partitionBy(['user_id', 'city']) \
                                                      .orderBy(F.desc('rank')))) \
                                        .withColumn('diff', F.datediff(F.col('max_date'), F.col('min_date'))) \
                                        .where('diff >= 27') \
                                        .select('user_id', 'city', 'diff', 'max_date') \
                                        .distinct() \
                                        .withColumn('rank_last_city', F.row_number().over(Window.partitionBy(['user_id',  'city']) \
                                                        .orderBy(F.desc('max_date')))) \
                                        .where('rank_last_city = 1') \
                                        .select('user_id',
                                                F.col('city').alias('home_city'))