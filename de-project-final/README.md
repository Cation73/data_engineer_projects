# Финальный проект DE

### Описание
Репозиторий предназначен для сдачи финального проекта.

### Структура репозитория
- `/src/dags/` - py файлы DAG
    - `s3_to_stg.py` - заливка данных из S3 в staging-layer
    - `stg_to_dds.py` - заливка данных из staging-layer в dds-layer
- `/src/img/` - скриншоты с дашборда
    - `metabase.png` - общий скриншот дашборда
- `/src/py/` - вспомогательные py файлы
    - `s3_connector.py` - вспомогательный файл подключения и скачивания данных из S3
- `/src/sql/` - sql файлы для создания таблиц и вставки данных
    - `ddl_currencies.sql` - запрос создания таблицы currencies
    - `ddl_global_metrics.sql` - запрос создания таблицы global_metrics
    - `ddl_transactions.sql` - запрос создания таблицы transactions
    - `dwh_global_metrics.sql` - запрос заливки данных из staging-layer в dds-layer
    
### Дашборд
![Иллюстрация к проекту](/src/img/metabase.png)


