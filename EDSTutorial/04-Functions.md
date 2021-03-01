List of functions that has already been covered in the other notebooks:
 - SAS Sum Function
 - SAS Basic Math
 - SAS Trim/Compress
 - SAS Rename
 - SAS mean, min, max

List of functions shown in this notebook:
 - SAS Upcase, Lowcase
 - SAS Combine text
 - SAS Substr
 - SAS Proc Transpose


# Load dataframe

``` python
%pyspark
df = runquery("select * from testNikka")
```


# Trim, upper, start/end with

``` python
%pyspark
from pyspark.sql.functions import col, lower 

print("*** Trim")
df.select(df.card_brand).groupby(df.card_brand).count().show()

df1 = df.withColumn("card_brand", trim(col("card_brand"))) \
                .withColumn("reason_code_description", trim(col("reason_code_description")))
df1.select(df1.card_brand).groupby(df1.card_brand).count().show()

print("*** issuer_country_code contains E")
df1.select(df1.lu_name, df1.issuer_country_code.alias('icc')).filter(df1.issuer_country_code.contains('E')).show(10)

print("*** issuer_country_code starts with E")
df1.select(df1.issuer_country_code).filter(df1.issuer_country_code.startswith('E')).show(10)

print("*** issuer_country_code ends with E")
df1.select(df1.issuer_country_code).filter(df1.issuer_country_code.endswith('E')).show(10)

print("*** lu_name lower")
df1.select(lower(df1.lu_name), df1.issuer_country_code.alias('icc')).filter(df1.issuer_country_code.endswith('E')).show(10)

print("*** lu_name substring")
df1.select(df1.lu_name, df1.lu_name.substr(1,3)).show(10)
```


# Transpose / Pivot

``` python
%pyspark
df_pivot = runquery("select * from testNikka")
df_pivot = df.select('lu_name', 'card_brand').groupby('lu_name', 'card_brand').count()
df_pivot.show()
df_pivot.groupby("card_brand").pivot("lu_name").sum("count").show()
```


# User Defined Functions

``` python
from pyspark.sql.functions import col 

def getCountry(code):
    thedict={"US":"United States","ITA":"Italy","ES":"Spain","FR":"France","IE":"Ireland","PRT":"Portugal","DEFAULT":"Not Found"}
    if(thedict.get(code) !=None):
        resStr = thedict.get(code)
    else:
        resStr = thedict.get("DEFAULT")
    return resStr

df_udf = df.withColumn("issuer_country_code", trim(col("issuer_country_code"))).groupby("issuer_country_code").sum("TRANSACTION_AMOUNT")
df_udf = df_udf.select("issuer_country_code", col('sum(TRANSACTION_AMOUNT)').alias('sumTransaction'))
```

 
# Use UDF with dataframe

``` python
%pyspark
from pyspark.sql.functions import col, udf
udfgetCountry = udf(getCountry)
df_udf.withColumn('country_desc',udfgetCountry(df_udf.issuer_country_code)).show()
```


# Register and use UDF with spark sql

``` python
%pyspark
spark.udf.register("getCountry", getCountry)
df_udf.createOrReplaceTempView("Test_Country")
spark.sql("select issuer_country_code, getCountry(issuer_country_code) as country_desc from Test_Country").show()
```


# Use UDF with spark sql

``` python
%spark.sql
select issuer_country_code, getCountry(issuer_country_code) as country_desc, sumTransaction from Test_Country WHERE NOT issuer_country_code IS NULL and issuer_country_code <> "";
```
