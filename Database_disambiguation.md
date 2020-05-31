1. Data lake.
Contains pretty much every kind of data not directly related to a transaction is stored here. The logic is, actions that a customer makes prior to a transaction can be potentially useful. The data stored here can potentially be anything, and may have large amounts of unstructured data. The index of the table is such a thing like a timestamp. When using time, start from highest to lowest, from year to minute and see at each level if it is enough. Even higher than that, data can be segregated by geographical region.
2. Database.
Contains data on separate parts of the business stored in separate tables. The data from real time transactions go here. As the data is fluid and is constantly being feed and read, it is not suitable for data analytics.
3. Data warehouse.
Contains older batches of incremental database data combined into a single table which makes it easier to run analytics on it. The process is called ETL (Extract Transform Load), where the data is extracted from the database, transformed, and loaded into the data warehouse. AWS redshift, snowflake and google bigquery are examples of cloud data warehouses.

pip install psycopg2
