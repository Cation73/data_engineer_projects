from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType,  DoubleType, TimestampType, IntegerType


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0"
        ]
    )
# реквизиты подключения для Kafka
kafka_host = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
                        }

topic_name_in = 'mr.sokolov.v.v_in'
topic_name_out = 'mr.sokolov.v.v_out'

# реквизиты подключения для PostgreSQL
postgre_host = 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de'
postgre_user = 'student'
postgre_password = 'de-student'

postgre_host_lh = 'jdbc:postgresql://localhost:5432/de'
postgre_user_lh = 'jovyan'
postgre_password_lh = 'jovyan'

# создание сессии Spark
def spark_init(name_init) -> SparkSession:
    spark = SparkSession.builder \
            .master("local") \
            .appName(name_init) \
            .config("spark.jars.packages", spark_jars_packages) \
            .getOrCreate()
        
    return spark

# стриминг данных по акциям
def read_promotions_stream(spark: SparkSession) -> DataFrame:
    df = spark.readStream.format('kafka') \
                .option('kafka.bootstrap.servers', kafka_host) \
                .option("subscribe", topic_name_in) \
                .option("maxOffsetsPerTrigger", 20) \
                .options(**kafka_security_options) \
                .load()
    
    return df

# чтение данных из PostgreSQL
def read_subscribers_restaurant(spark: SparkSession) -> DataFrame:
    sub_rest_df = spark.read \
                    .format("jdbc") \
                    .option("url", postgre_host) \
                    .option("dbtable", "subscribers_restaurants") \
                    .option("driver", "org.postgresql.Driver") \
                    .option('user', postgre_user) \
                    .option('password', postgre_password) \
                    .load()
 
    return sub_rest_df

# обработка данных
def transform_kafka_data(kafka_df) -> DataFrame:
    # схема входного сообщения для json
    incomming_message_schema = StructType([
            StructField("restaurant_id", StringType()),
            StructField("adv_campaign_id", StringType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", LongType()),
            StructField("adv_campaign_datetime_end", LongType()),
            StructField("datetime_created", LongType())
                        ])
    
    # десериализация value сообщения json 
    df = kafka_df.withColumn('value', f.col('value').cast(StringType())) \
                .withColumn('event', f.from_json(f.col('value'), incomming_message_schema)) \
                .select(f.col('event.restaurant_id').alias('restaurant_id'),
                        f.col('event.adv_campaign_id').alias('adv_campaign_id'),
                        f.col('event.adv_campaign_content').alias('adv_campaign_content'),
                        f.col('event.adv_campaign_owner_contact').alias('adv_campaign_owner_contact'),
                        f.col('event.adv_campaign_datetime_start').alias('adv_campaign_datetime_start'),
                        f.col('event.adv_campaign_datetime_end').alias('adv_campaign_datetime_end'),
                        f.col('event.datetime_created').alias('datetime_created'))

    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    # фильтрация по времени старта и окончания акции
    filtered_read_stream_df = df.filter(f'adv_campaign_datetime_start <= {current_timestamp_utc} and adv_campaign_datetime_end >= {current_timestamp_utc}')

    return filtered_read_stream_df

# подготовка конечного датафрейма для заливки в PostgreSQL и Kafka
def join_kafka_postgre(promotions_df, restaurant_df) -> DataFrame:
    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    # join и дедубликация
    join_df = promotions_df.join(restaurant_df.select('restaurant_id', 'client_id'),
                                 how = 'inner', on = 'restaurant_id') \
                            .withColumn('trigger_datetime_created', f.lit(current_timestamp_utc).cast(LongType())) \
                            .withColumn('datetime_created_dupl', f.from_unixtime(f.col('datetime_created') / 1000).cast(TimestampType())) \
                            .withWatermark('datetime_created_dupl', '1 minutes') \
                            .dropDuplicates(['restaurant_id', 'adv_campaign_id', 'client_id'])
    

    return join_df

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):

    # сохраняем датафрейм в оперативную память 
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    df.withColumn('feedback', f.lit(None).cast(StringType())) \
        .write \
        .mode('append') \
        .format('jdbc') \
        .option("url", postgre_host_lh) \
        .option("dbtable", "subscribers_feedback") \
        .option("driver", "org.postgresql.Driver") \
        .option('user', postgre_user_lh) \
        .option('password', postgre_password_lh) \
        .save()  
     
    # создаём df для отправки в Kafka. Сериализация в json.
    df_kafka = df.withColumn('value',
                       f.to_json(f.struct(f.col('restaurant_id'),
                                          f.col('adv_campaign_id'),
                                          f.col('adv_campaign_content'),
                                          f.col('adv_campaign_owner_contact'),
                                          f.col('adv_campaign_datetime_start'),
                                          f.col('adv_campaign_datetime_end'),
                                          f.col('datetime_created'),
                                          f.col('client_id'),
                                          f.col('trigger_datetime_created')))) \
                            .select(f.col('trigger_datetime_created').cast(StringType()).alias('key'),
                                    f.col('value'))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_kafka.write \
        .mode('append') \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_host) \
        .options(**kafka_security_options) \
        .option('topic', topic_name_out) \
        .save()
    
    # очищаем память от df
    df.unpersist()

 
# запуск процесса
if __name__ == "__main__":

    spark = spark_init('project_8')

    read_kafka_prom = read_promotions_stream(spark)
    read_postgre_sub = read_subscribers_restaurant(spark)

    transform_kafka_prom = transform_kafka_data(read_kafka_prom)
    result_df = join_kafka_postgre(transform_kafka_prom, read_postgre_sub)

    result_df.writeStream \
            .foreachBatch(foreach_batch_function) \
            .start() \
            .awaitTermination()






