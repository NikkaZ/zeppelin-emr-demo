# Create a dataframe from oracle

``` python
%pyspark
from pyspark.sql.functions import trim, col

df = runquery("select * from testNikka")

df_graphs = df.withColumn("card_brand", trim(col("card_brand"))) \
                .withColumn("reason_code_description", trim(col("reason_code_description")))
df_graphs = df_graphs.select("lu_name", "card_brand", "reason_code_description", "dcc_bill_amount","transaction_amount")
df_graphs.show()
```


# Show graph

``` python
%spark.sql
select card_brand, reason_code_description,dcc_bill_amount,transaction_amount from Transactions 
```


# Create a dataframe from S3

``` python
%pyspark
dfs3 = spark.read.load("s3a://eds-psf-s3-test-rbs-conformed/atm/rbs-atm-fault-test-conformed/year=2020/month=07/day=17/run-datasink4-7-part-block-0-r-00000-snappy.parquet")
dfs3.show(6)

dfs3.createOrReplaceTempView("Fault")
```


# Show graph

``` python
%spark.sql
select name, downtime_pot,fault_description from Fault
```


# Read multiple dataset

``` python
%pyspark
import pyspark.sql.functions as func
from pyspark.sql.functions import col, unix_timestamp, to_date

dfs3_2010 = spark.read.load("s3a://eds-psf-s3-test-rbs-conformed/atm/rbs-atm-transaction-test-conformed/year=2021/month=03/day=01/*.parquet")

dfs3_2010 = dfs3_2010.withColumn("card_brand", trim(col("card_brand"))) \
            .withColumn("lu_name", trim(col("lu_name")))

dfs3_2010 = dfs3_2010.groupby("lu_name", "card_brand").sum("transaction_amount").select("lu_name", "card_brand", func.col('sum(transaction_amount)').alias('sumtransaction'))

dfs3_2020 = spark.read.load("s3a://eds-psf-s3-test-rbs-conformed/atm/rbs-atm-transaction-test-conformed/year=2030/month=07/day=15/*.parquet")

dfs3_2020 = dfs3_2020.withColumn("card_brand", trim(col("card_brand"))) \
            .withColumn("lu_name", trim(col("lu_name")))
dfs3_2020 = dfs3_2020.groupby("lu_name", "card_brand").sum("transaction_amount").select("lu_name", "card_brand", func.col('sum(transaction_amount)').alias('sumtransaction'))

dfs3_2010.createOrReplaceTempView("Transactions_2010")
dfs3_2020.createOrReplaceTempView("Transactions_2020")
```


# Show graph

``` python
%spark.sql
select t2010.LU_NAME, t2010.CARD_BRAND, t2010.sumtransaction as sum2010, t2020.sumtransaction as sum2049 from Transactions_2010 as t2010
inner join Transactions_2020 as t2020 on t2010.LU_NAME = t2020.LU_NAME and t2010.CARD_BRAND = t2020.CARD_BRAND
```
