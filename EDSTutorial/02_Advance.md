List of functions shown in this notebook:
 - create a new dataframe based on the existing one
 - SQL Create Table, Alter Table
 - SQL Create temporary view
 - SQL Delete = filter columns
 - SQL Joins, Inner, Left, Right, Full (Outer), Self
 - SQL Union
 - SQL Null Values
 - SQL In, Between

List of functions that can be achieved in different ways:
 - SQL Insert Into (example needed)
 - SQL Update (example needed)
 - SQL Select Into (select + create a new dataframe)
 - SQL Nested Queries (doesn't support subqueries in WHERE clause but can achieved in different ways)
 - SQL Drop Table (overwrite)
 
List of functions to have a look at:
 - SQL Pass-Thru
 - SQL Index


# Load Spark Dataframe

``` python
%pyspark
df = runquery("select * from testNikka")
```


# Create new dataframe

``` python
%pyspark
from pyspark.sql.functions import trim
from pyspark.sql.functions import col

print("*** Each result can be shown or saved in the same or another dataframe")
df.groupby('card_brand').count().show()

print("*** New dataframe with changed data (trim)")
df1 = df.withColumn("card_brand", trim(col("card_brand")))
df1.groupby('card_brand').count().show()

print("*** For filters we can apply one filter, save the result in another dataframe and then apply another filter")
df1 = df.filter(('card_brand = "VISA"'))
df1.groupby('card_brand','lu_name').count().show()
df1.select('card_brand','lu_name').filter('lu_name = "TE37696" or lu_name = "TE33806"').drop_duplicates(['card_brand']).show() 
```


# Look if a table exists

``` python
%pyspark
from pyspark.sql.functions import lower, upper

table_tolook = "test"
table_list=showtables()
table_name=table_list.filter(lower(table_list.TABLE_NAME)==table_tolook).collect()
if len(table_name)>0:
    print("table found " + table_tolook)
else:
    print("table not found " + table_tolook)
    
table_tolook = "testtest"
table_name=table_list.filter(lower(table_list.TABLE_NAME)==table_tolook).collect()
if len(table_name)>0:
    print("table found " + table_tolook)
else:
    print("table not found " + table_tolook)
```


# Create/Overwrite table

``` python
%pyspark
from pyspark.sql.functions import col
df = runquery("select * from testNikka")
dftable = df.select("lu_name", "card_brand", "dcc_bill_amount").groupBy("lu_name", "card_brand").sum("dcc_bill_amount").select("lu_name", "card_brand", col('sum(dcc_bill_amount)').alias('sumAmount'))
createtablefromdf(dftable, "NZ_test","overwrite")
df_NZ = runquery("select * from NZ_test")
print("*** Overwrite table if exists")
print(df_NZ.count())
# append: Append contents of this :class:DataFrame to existing data.
# overwrite: Overwrite existing data.
# ignore: Silently ignore this operation if data already exists.
# error (default case): Throw an exception if data already exists
createtablefromdf(dftable, "NZ_test","append")
df_NZ = runquery("select * from NZ_test")
print("*** Append to an existing table if exists")
print(df_NZ.count())
```


# Create a temporary table

``` python
%pyspark
import pyspark.sql.functions as func
dftable = df.select("lu_name", "card_brand","transaction_date_time", "dcc_bill_amount", "transaction_amount") \
        .groupBy("lu_name", "card_brand","transaction_date_time").sum("dcc_bill_amount","transaction_amount") \
        .select("lu_name", "card_brand","transaction_date_time", func.col('sum(dcc_bill_amount)').alias('sumAmount'), col('sum(transaction_amount)').alias('sumTransaction'))
createtablefromdf(dftable, "NZ_test","overwrite")
df_NZ = runquery("select * from NZ_test")
df_NZ.show()
```


# Drop a column

``` python
%pyspark
df_drop = df_NZ.drop(df_NZ.sumAmount)
df_drop.show()
```


# Union dataframe

``` python
%pyspark
from pyspark.sql.functions import trim
df_union1 = df.withColumn("card_brand", trim(col("card_brand")))
df_union2 = df_union1.select("lu_name", "card_brand", "dcc_bill_amount","transaction_amount").groupby("lu_name", "card_brand").sum("dcc_bill_amount","transaction_amount").where(df_union1.card_brand  == "VISA")
df_union3 = df_union1.select("lu_name", "card_brand", "dcc_bill_amount","transaction_amount").groupby("lu_name", "card_brand").sum("dcc_bill_amount","transaction_amount").where(df_union1.card_brand  == "MASTER")
df_union2.show()
df_union3.show()
df_concat = df_union2.union(df_union3)
df_concat.show()
```


# Create tables for join examples

``` python
%pyspark
columns = ["id","Name", "Surname"]
data = [("1", "John", "Smith"),
    ("2", "Paul", "Smith"),
    ("4", "Tom", "Cruise")]

df1 = spark.createDataFrame(data=data,schema=columns)

df1.show()
createtablefromdf(df1, "NZ_df1","overwrite")

columns = ["id", "Profession", "Salary"]
data = [("1", "Lawyer", "20000"),
    ("3", "Teacher", "10000"),
    ("4", "Actor", "30000")]

df2 = spark.createDataFrame(data=data,schema=columns)

df2.show()
createtablefromdf(df2, "NZ_df2","overwrite")

columns = ["id", "City"]
data = [("1", "Edinburgh"),
    ("2", "Inverness"),
    ("4", "North Berwick")]

df3 = spark.createDataFrame(data=data,schema=columns)

df3.show()
createtablefromdf(df3, "NZ_df3","overwrite")
```


# Join example

``` python
%pyspark
print("*** inner = condition")
df_join  = df1.join(df2, df1.id == df2.id, 'inner')
df_join.show()

print("*** inner > condition")
df_join  = df1.join(df2, df1.id > df2.id, 'inner')
df_join.show()

print("*** full outer")
df_join  = df1.join(df2, df1.id == df2.id, 'outer')
df_join.show()

print("*** left")
df_join  = df1.join(df2, df1.id == df2.id, 'left')
df_join.show()

print("*** right")
df_join  = df1.join(df2, df1.id == df2.id, 'right')
df_join.show()

print("*** self")
df_join  = df1.join(df1, [df1.Surname == df1.Surname, df1.id != df1.id], 'inner')
df_join.show()

print("*** self with alias")
df_as1 = df1.alias("df_as1")
df_as2 = df1.alias("df_as2")
df_join  = df_as1.join(df_as2, [df_as1.Surname == df_as2.Surname, df_as1.id != df_as2.id], 'inner')
df_join.show()
```


# Particular join examples

``` python
%pyspark
from pyspark.sql.functions import col 

print("*** left_anti it selects all rows from df1 that are not present in df2")
df_join  = df1.join(df2, df1.id == df2.id, 'left_anti')
df_join.show()


print("*** left_semi unlike the left outer join, the result contains only the columns brought by the left dataset")
df_join  = df1.join(df2, df1.id == df2.id, 'left_semi')
df_join.show()


print("*** cross join")
df_join = df1.crossJoin(df2)
df_join.show()

print("*** join multiple tables first method")
df1.createOrReplaceTempView('df1')
df2.createOrReplaceTempView('df2')
df3.createOrReplaceTempView('df3')
query = "select df1.*, df2.profession, df2.salary, df3.city " \
        "from df1 " \
        "left join df2 on df1.id=df2.id "\
        "left join df3 on df1.id=df3.id"
joined_df = spark.sql(query)
joined_df.show()

print("*** join multiple tables second method")
df1 = df1.alias('df1')
df2 = df2.alias('df2')
df3 = df3.alias('df3')

joined_df = df1.join(df2, col('df1.id') == col('df2.id'), 'left') \
    .join(df3, col('df1.id') == col('df3.id'), 'left') 
joined_df.show()

print("*** join multiple tables second method with select")
joined_df = df1.join(df2, col('df1.id') == col('df2.id'), 'left') \
    .join(df3, col('df1.id') == col('df3.id'), 'left') \
    .select('df1.*', 'df2.profession', 'df2.salary', 'df3.city')
joined_df.show()
```


# Example with null

``` python
%pyspark
from pyspark.sql.functions import col 
df_join  = df1.join(df2, df1.id == df2.id, 'outer').select("Name", "Surname", "Profession", "Salary")
print("*** initial table")
df_join.show()

print("*** filter where name is null")
df_join.where(col("Name").isNull()).show()

print("*** filter where name is not null")
df_join.where(col("Name").isNotNull()).show()

print("*** drop rows where there is a null")
df_join.na.drop().show()

print("*** drop rows where there is a null in the name column")
df_join.na.drop(subset=["Name"]).show()

print("*** replace null value")
df_join.na.fill("empty").show()
df_join.na.fill("").show()
```


# Column: add/remove/calculated column/change type of column

``` python
%pyspark
from pyspark.sql.functions import lit, when

print("*** add new column")
df_addcol = df_concat.withColumn('new_column', lit('This is a new column'))
df_addcol.show()

print("*** Rename column")
df_addcol = df_addcol.withColumnRenamed('new_column', 'SUM')
df_addcol = df_addcol.select("lu_name", "card_brand",func.col('sum(dcc_bill_amount)').alias('sumAmount'), func.col('sum(transaction_amount)').alias('sumTransaction'), "SUM")
df_addcol.show(5)

print("*** Add calculated column")
new_df = df_addcol.withColumn('SUM', df_addcol.sumAmount + df_addcol.sumTransaction)
new_df.show(5)
print("*** Original data type")
display(new_df)

print("")
print("")
print("*** Change type of column")
changedTypedf = new_df.withColumn("SUM", new_df["SUM"].cast("String"))
display(changedTypedf)

print("")
print("")
print("*** Change type of column creating a new column")
changedTypedf = new_df.withColumn("SUM_STR", new_df["SUM"].cast("String"))
display(changedTypedf)

print("")
print("")
print("*** Add column with condition")
new_df.withColumn('Indicator', when(
    (col("sumAmount") > 40000) & (col("sumAmount")  <=100000), 4
    ).when(col("sumAmount") > 100000, 2).otherwise(0)).show()
```


# Create dataframe from scratch

``` python
%pyspark
columns = ["lu_name","card_brand", "reason_code_description", "dcc_bill_amount", "transaction_amount"]
data = [("TE99999", "RBS", "DEFAULT",0,0),
    ("TE99999", "VISA", "DEFAULT",0,0),
    ("TE99999", "VISA", "EXPIRED",0,0)]

df_newrow = spark.createDataFrame(data=data,schema=columns)

from pyspark.sql import Row
df_newrow1 = sc.parallelize([ \
    Row(lu_name='TE88888', card_brand='VISA', reason_code_description='DEFAULT', dcc_bill_amount=0, transaction_amount=0), \
    Row(lu_name='TE88888', card_brand='VISA', reason_code_description='EXPIRED', dcc_bill_amount=0, transaction_amount=0), \
    Row(lu_name='TE88888', card_brand='RBS', reason_code_description='DEFAULT', dcc_bill_amount=0, transaction_amount=0)]).toDF()
```


# Filter between/in

``` python
%pyspark
from pyspark.sql.functions import col
dfbetween = df.select("lu_name", "card_brand","transaction_date_time", "dcc_bill_amount", "transaction_amount") \
        .groupBy("lu_name", "card_brand","transaction_date_time").sum("dcc_bill_amount","transaction_amount") \
        .select("lu_name", "card_brand","transaction_date_time", col('sum(dcc_bill_amount)').alias('sumAmount'), col('sum(transaction_amount)').alias('sumTransaction'))

print("*** Filter using between")
dfbetween.select("lu_name", "card_brand","transaction_date_time","sumAmount","sumTransaction", dfbetween.sumTransaction.between(0, 100).alias('between')).filter('between').show()

dfbetween.select("lu_name", "card_brand","transaction_date_time","sumAmount","sumTransaction", dfbetween.transaction_date_time.between('2006-01-01', '2006-12-31').alias('between')).filter('between').show()

print("*** Filter using in")
dfbetween.where(col("card_brand").isin({"NATWES", "LINK"})).show()
```
