import pandas as pd
import snowflake.connector

cnn = snowflake.connector.connect(user = 'TAVILY_HOME_ASSIGNMENT',
                                  password = 'MakeYourTimeMeaningful',
                                  account = 'zqgisxr-yab27380',
                                  database = 'TAVILY_HOME_ASSIGNMENT',
                                  warehouse = 'COMPUTE_WH',
                                  schema = 'SNOWFLAKE_AIRFLOW_DBT',
                                  )
cs = cnn.cursor()

sql = 'select * from STG_USAGE'
cs.execute(sql)
df = cs.fetch_pandas_all()
cs.close()
cnn.close()
