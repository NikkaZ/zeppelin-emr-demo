List of functions shown in this notebook:
 - Show tables in a db
 - Show schema for a table
 - SQL Select
 - SQL Select Distinct
 - SQL Where
 - SQL And Or Not
 - SQL Order By
 - SQL Min and Max
 - SQL Count, Avg, Sum
 - SQL Group By
 - SQL Having
 - SQL Like
 - SQL Wildcards


# Show tables and schema

``` python
%pyspark
showtablesnames()
showschema("testNikka")
```


# Load Spark Dataframe

``` python
%pyspark
df = runquery("select * from testNikka")
```


# Select example

``` python
%pyspark
print("*** Select some columns")
df.select('lu_name', 'card_brand', 'dcc_bill_amount').show(3)

print("*** Select some columns order by")
df.select('lu_name', 'card_brand', 'dcc_bill_amount').orderBy('lu_name').show(3)

print("*** Select some columns sort. Order by is an alias of sort function https://stackoverflow.com/questions/40603202/what-is-the-difference-between-sort-and-orderby-functions-in-spark")
df.select('lu_name', 'card_brand', 'dcc_bill_amount').sort('lu_name').show(3)

print("*** Select all but some columns")
cols = list(set(df.columns) - {'note_denom_20', 'YEAR'})
df.select(cols).show(3)
```


# Select distinct example

``` python
%pyspark
print("*** Normal distinct")
df.select('card_brand', 'issuer_institution_name').distinct().orderBy('card_brand').show()

print("*** drop duplicates, doesn't consider the other columns")
df.select('card_brand', 'issuer_institution_name').drop_duplicates(['card_brand']).orderBy('card_brand').show()

print("*** so this function needs to be carefully used. This an example that gives the same result of distinct")
df.select('card_brand', 'issuer_institution_name').drop_duplicates(['card_brand', 'issuer_institution_name']).orderBy('card_brand').show()
```


# Filter/group/order example

``` python
%pyspark
print("*** Filter selecting all columns")
df.filter(df.lu_name == "TE20116").show(2)

print("*** Filter with and")
df.select('lu_name', 'card_brand', 'dcc_bill_amount').filter( (df.card_brand  == "VISA") & (df.lu_name == "TE20116") ).show(2)

print("*** Filter with and and or first method")
df.select('lu_name', 'card_brand', 'dcc_bill_amount').filter(('card_brand = "VISA"')).filter('lu_name == "TE33806" or lu_name = "TE37696"').drop_duplicates(['lu_name']).show()

print("*** Filter with and and or second method")
df.select('lu_name', 'card_brand', 'dcc_bill_amount').filter('card_brand = "VISA" AND (lu_name == "TE33806" or lu_name = "TE37696")').drop_duplicates(['lu_name']).show()

print("*** Group by count")
df.groupby('card_brand').count().show()

print("*** Group by count and order by desc")
df.groupby('card_brand').count().orderBy(df.card_brand.desc()).show()

print("*** Order by asc")
df.select(df.lu_name, df.issuer_country_code.alias('ICC')).orderBy(df.issuer_country_code.asc()).show()

print("*** Order by asc nulls last")
df.select(df.lu_name, df.issuer_country_code.alias('ICC')).orderBy(df.issuer_country_code.asc_nulls_last()).show()
```


# More group example

``` python
%pyspark
from pyspark.sql.functions import col
import pyspark.sql.functions as func

print("*** Group by sum")
df.groupby('card_brand').sum('dcc_bill_amount').show()

print("*** Group by alias")
df.groupby('card_brand').sum('dcc_bill_amount').select('card_brand', func.col('sum(dcc_bill_amount)').alias('sumAmount')).show()

print("*** Group by alias and filter > 0")
df.groupby('card_brand').sum('dcc_bill_amount').select('card_brand', func.col('sum(dcc_bill_amount)').alias('sumAmount')).where(col('sum(dcc_bill_amount)')>0).show()
```


# More filter example

``` python
%pyspark
print("*** Filter = ")
df.select('lu_name').filter(df.lu_name == "TE20116").show(2)

print("*** Filter like first method")
df.select('lu_name').where(col('lu_name').like("%TE%")).show(2)

print("*** Filter like second method")
df.select('lu_name').where("lu_name like '%TE%'").groupby('lu_name').count().show()

print("*** Filter with regolar expression for all the wildcards and more")
expression = r'TE3..96'
df.filter(df['lu_name'].rlike(expression)).groupby('lu_name').count().show()
```


# Easy statistics

``` python
%pyspark
print("*** Count")
print(df.count())
print("")
print("*** Number columns and names")
print(len(df.columns), df.columns)
print("")
print("*** Summary statistics (mean, standard deviance, min, max, count) of numerical columns")
df.describe().show()
print("")
print("*** Summary statistics on one or more column")
df.describe('transaction_amount','note_denom_05','margin','reversal_amount','original_amount','dcc_bill_amount').show()
```


# Other statistics

``` python
%pyspark
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col, min, max

print("*** Second method")
df.groupby('card_brand').agg({'dcc_bill_amount': 'mean'}).show()

print("")
print("*** Third method")
df_stats = df.select(
    _mean(col('dcc_bill_amount')).alias('mean'),
    _stddev(col('dcc_bill_amount')).alias('std')
).collect()

mean = df_stats[0]['mean']
std = df_stats[0]['std']
print("Mean " + str(mean), "Standard deviation " + str(std))

print("")
print("*** Forth method")
df.select([_mean('dcc_bill_amount'), min('dcc_bill_amount'), max('dcc_bill_amount')]).show()
```
