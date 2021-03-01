# NOTE

This notebook uses pre-signed POST requests. 
The request needs to be generate first and the upload form updated with the pre-signed POST request details.
Please run the first two paragrahs before uploading to s3


# Generate pre-signed POST request

``` python
%spark.pyspark

bucket_name="eds-psf-s3-test-spark-cluster-dataeng-build"
object_name="uploads/${filename}"
fields=None
conditions=None
expiration=3600

import boto3

s3_client = boto3.client('s3', region_name="eu-west-1", config=boto3.session.Config(signature_version='s3v4'))

response = s3_client.generate_presigned_post(
    bucket_name,
    object_name,
    Fields=fields,
    Conditions=conditions,
    ExpiresIn=expiration
)

url=response['url']
z.remove(url)
z.put("url", url)

key=response['fields']['key']
z.put("key", key)

x_amz_credential=response['fields']['x-amz-credential']
z.put("x_amz_credential", x_amz_credential)

x_amz_security_token=response['fields']['x-amz-security-token']
z.put("x_amz_security_token", x_amz_security_token)

policy=response['fields']['policy']
z.put("policy", policy)

x_amz_signature=response['fields']['x-amz-signature']
z.put("x_amz_signature", x_amz_signature)

x_amz_date=response['fields']['x-amz-date']
z.put("x_amz_date", x_amz_date)

#x-amz-algorithm

print(response)
```


# Upload file to S3 bucket

``` python
%spark
{
val url=z.get("url")
z.angularBind("url", url)

val object_prefix=z.get("object_prefix")
z.angularBind("object_prefix", object_prefix)

val policy=z.get("policy")
z.angularBind("policy", policy)

val x_amz_credential=z.get("x_amz_credential")
z.angularBind("x_amz_credential",x_amz_credential)

val x_amz_security_token=z.get("x_amz_security_token")
z.angularBind("x_amz_security_token",x_amz_security_token)

val x_amz_signature=z.get("x_amz_signature")
z.angularBind("x_amz_signature",x_amz_signature)

val x_amz_date=z.get("x_amz_date")
z.angularBind("x_amz_date",x_amz_date)


println("""
%angular
<html>
<script>
    function showname () {
      var file_name = document.getElementById('file_input').files.item(0).name; 
      document.getElementById('key_input').value="uploads/"+file_name;
    };
</script>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
  </head>
  <body>
  <form action="https://eds-psf-s3-test-spark-cluster-dataeng-build.s3.amazonaws.com/" method="post" enctype="multipart/form-data">
    <input id="key_input" type="hidden" name="key" value="" /><br />
    <input type="hidden" name="X-Amz-Algorithm" value="AWS4-HMAC-SHA256" />
    <input type="hidden" name="x-amz-security-token" value="{{x_amz_security_token}}" />
    <input type="hidden" name="X-Amz-Credential" value="{{x_amz_credential}}" />
    <input type="hidden" name="X-Amz-Date" value="{{x_amz_date}}" />
    <input type="hidden" name="X-Amz-Signature" value="{{x_amz_signature}}" />
    <input type="hidden" name="Policy" value="{{policy}}" />
    <input  id="file_input" type="file" name="file" onChange="showname()"/>
    <input type="submit" name="submit" value="Upload to Amazon S3" />
  </form>
 
</html>
""")
}
```


# Get list of file in a bucket

``` python
%pyspark
import pprint 
dictfiles = get_matching_s3_keys(bucket='eds-psf-s3-test-spark-cluster-dataeng-build', prefix='', suffix='.csv')
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(dictfiles)
```


# Load a panda dataframe

``` python
%pyspark
import pandas as pd
 
df = pd.read_csv('s3a://eds-psf-s3-test-spark-cluster-dataeng-build/uploads/daily_faultdata_20970909.csv', sep=',', header=0)
df
```


# Load a spark dataframe and create a table on oracle

``` python
%pyspark
df = spark.read.format('csv').options(header='true').load("s3a://eds-psf-s3-test-spark-cluster-dataeng-build/uploads/daily_faultdata_20970909.csv")
df.count()
createtablefromdf(df, "daily_faultdata", "overwrite")
```


# Query table on oracle

``` python
%teds0db
SELECT TABLE_NAME FROM all_tables WHERE TABLESPACE_NAME = 'SVC_DATAENG_BUILD_TEST_TBS' order by TABLE_NAME
```


# Load spark dataframe from oracle

``` python
%pyspark
df = runquery("select * from DAILY_FAULTDATA")
```


# Create CSV from SQL

``` python
%pyspark
import pandas as pd
from IPython.display import HTML
from pyspark.context import SparkContext


def create_download_link( df, title = "Download CSV file", filename = "daily_faultdata.csv"):  
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload,title=title,filename=filename)
    return HTML(html)

df_download = df.select('Device ID', 'Duration', 'estate', 'severity', 'Downtime')
df_download.show(3)

df_download.printSchema()
df_download.show(truncate=False)
pdf=df_download.toPandas()

create_download_link(pdf)
```
