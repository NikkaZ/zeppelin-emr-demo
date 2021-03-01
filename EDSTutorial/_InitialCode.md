# Connection with oracle db

``` python
%pyspark
import boto3
from botocore.config import Config
ssm = boto3.client("ssm", region_name="eu-west-1",config=Config(proxies={"https": "proxy.service:8080"}))

oracle_host="tcps://teds0db.cibbtwlv7dnd.eu-west-1.rds.amazonaws.com"
oracle_port="2484"
oracle_database="TEDS0DB"
oracle_url="jdbc:oracle:thin:@" + oracle_host + ":" + oracle_port+ "/" + oracle_database + "?javax.net.ssl.trustStore=/usr/lib/oracle/19.6/client64/network/admin/wallet/cwallet.sso"
oracle_username="SVC_DATAENG_BUILD_TEST"
oracle_password = ssm.get_parameter(Name='/rds/oracle/' + oracle_database + '/' + oracle_username + '_password', WithDecryption=True)['Parameter']['Value']
```

# Common functions oracle

``` python
%pyspark
def showschema(table):
    df = spark.read \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", table) \
        .option("user", oracle_username) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    return df.printSchema()

def runquery(query):
    return spark.read \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("query", query) \
        .option("user", oracle_username) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

def showtables():
    return spark.read \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("query", "SELECT * FROM   all_tables WHERE TABLESPACE_NAME = 'SVC_DATAENG_BUILD_TEST_TBS'") \
        .option("user", oracle_username) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

def showtablesnames():
    df = spark.read \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("query", "SELECT TABLE_NAME FROM   all_tables WHERE TABLESPACE_NAME = 'SVC_DATAENG_BUILD_TEST_TBS'") \
        .option("user", oracle_username) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    return df.select("TABLE_NAME").show()

def createtablefromdf(df, tablename, method):
    df.write.format("jdbc") \
        .mode(method) \
        .option("url", oracle_url) \
        .option("dbtable", tablename) \
        .option("user", oracle_username) \
        .option("password", oracle_password) \
        .save()
 ```       
        
# Get list of files in S3 bucket

``` python
%pyspark
import boto3
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
import ntpath
def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    
    s3resource = boto3.resource("s3", region_name="eu-west-1",config=Config(proxies={"https": "proxy.service:8080"}))
    #buckets = [bucket.name for bucket in s3.buckets.all()]
    #print(buckets)
    s3client = boto3.client("s3", region_name="eu-west-1",config=Config(proxies={"https": "proxy.service:8080"}))
    dict_files={}
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                head, tail = ntpath.split(key)
                if head not in dict_files:
                    dict_files[head] = [tail]
                else:
                    dict_files[head].append(tail)
        return dict_files
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
```
