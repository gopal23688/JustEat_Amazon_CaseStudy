# JustEat_Amazon_CaseStudy
# Below is the code snnippet for loading into S3 Consumption Buckets after cleansing the data from raw .

1) to cleanse the raw file and load into s3 staging buckets ( consumption )

Fact table Code :

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sc2 = SparkSession.builder.appName("fct_amazon_review").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g") .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

##@params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##Source Files to Load into redshift

## @type: DataSource
## @args: [database = "reviews", table_name = "meta_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
metadata = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "meta_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")
## @type: DataSource
## @args: [database = "reviews", table_name = "reviews_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
reviews = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "reviews_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")


##Formating Metadata table columns as per target needs

##Removing Un-necessary columns for the load and creating metadata view

metadata.printSchema()

metadata.toDF().select ("price","asin").createOrReplaceTempView("metadata_view")


##data frame for reviews table

 
reviews.toDF().select ("asin","overall","reviewerID").createOrReplaceTempView("reviews_view")


##Using SQL to load into s3 staging area

l_history=spark.sql("SELECT CURRENT_TIMESTAMP as tc_insert_dt,b.asin as product_id,b.overall as overall_rating,  b.reviewerID as reviewer_id,a.price as product_price from metadata_view a inner join reviews_view b on a.asin=b.asin where a.asin is not null")

##Repartitioning the data frame to give output as single file

partitioned_dataframe = l_history.repartition(1)

##Converting data frame to dyanamic frame using fromDF function

output=DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_dataframe")

##Writing output to S3 Bucket


glueContext.write_dynamic_frame.from_options(frame = output, connection_type = "s3", connection_options = {"path": "s3://justeatconsumption/"},format = "csv", format_options = {"writeHeader": True}, transformation_ctx = "datasink2")



job.commit()



```

Dimmension Table Code into S3: dim_reviews

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sc2 = SparkSession.builder.appName("dim_amazon_reviews").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g") .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

##@params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##Source Files to Load into redshift

## @type: DataSource
## @args: [database = "reviews", table_name = "meta_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
metadata = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "meta_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")
## @type: DataSource
## @args: [database = "reviews", table_name = "reviews_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
reviews = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "reviews_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")





##Formating Reviews table columns as per target needs

##Removing Un-necessary columns for the load and creating metadata view

reviews.printSchema()

#removes comma from the column reviewTime

 
df=reviews.toDF().withColumn('reviewTime', regexp_replace('reviewTime', ',', ''))

#step2 to replace white spaces with dashes to look in date format  
  
df2=df.withColumn('reviewTime', regexp_replace('reviewTime', ' ', '-'))


##data frame for reviews table

 
df2.createOrReplaceTempView("reviews_view")


##Using SQL to load into s3 staging area

l_history=spark.sql("select CURRENT_TIMESTAMP as TC_INSERT_DT ,reviewerID as reviewer_id,reviewerName as reviewer_name,summary as reviewer_summary, reviewTime as review_date,cast(unixReviewTime as timestamp) as review_timestamp from reviews_view")

##Repartitioning the data frame to give output as single file

partitioned_dataframe = l_history.repartition(1)

##Converting data frame to dyanamic frame using fromDF function

output=DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_dataframe")

##Writing output to S3 Bucket


glueContext.write_dynamic_frame.from_options(frame = output, connection_type = "s3", connection_options = {"path": "s3://reviewsdimconsumption/"},format = "csv", format_options = {"writeHeader": True}, transformation_ctx = "datasink2")



job.commit()

```

Dimmension Table Code into S3 : dim_amazon_product 

```

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode
from pyspark.sql.functions import col, concat_ws


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sc2 = SparkSession.builder.appName("dim_amazon_metadata").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g") .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

##@params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##Source Files to Load into redshift

## @type: DataSource
## @args: [database = "reviews", table_name = "meta_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
df = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "meta_movies_and_tv_json", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")

##Exploding the categories using explode function


#df2=df.toDF().select(asin,brand,description,imUrl,price,title,explode(categories))

#df2=df.toDF().select(asin,explode(df.categories),description,title,price,brand,imUrl)

df2=df.toDF().withColumn("col",explode("categories"))

## Converting the exploded col column into string

df2.printSchema()

metadata = df2.withColumn("col",concat_ws(",",col("col")))


metadata.printSchema()



##data frame for metadata table

 
metadata.createOrReplaceTempView("metadata_view")


##Using SQL to load into s3 staging area

l_history=spark.sql("select distinct current_timestamp as tc_insert_dt,asin as product_id,title as product_name,imUrl as product_url,brand as brand_name,col as brand_category,description as brand_description from metadata_view")

##Repartitioning the data frame to give output as single file

partitioned_dataframe = l_history.repartition(1)

##Converting data frame to dyanamic frame using fromDF function

output=DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_dataframe")

##Writing output to S3 Bucket


glueContext.write_dynamic_frame.from_options(frame = output, connection_type = "s3", connection_options = {"path": "s3://metadatadimconsumption/"},format = "csv", format_options = {"writeHeader": True}, transformation_ctx = "datasink2")



job.commit()

```

# Below is the code snippet to rename the file which is in spark naming format and also archiving the files to archiving consumption s3 bucket.

```
import datetime

import boto3

currentdate = datetime.datetime.now().strftime("%Y-%m-%d")

print(currentdate)

client = boto3.client('s3')

#bucket name

BUCKET_NAME = 'metadatadimconsumption'

#backup bucket name

BACKUP_BUCKET_NAME='justeatconsumptionarchive'

response = client.list_objects(
Bucket=BUCKET_NAME,
)


name = response["Contents"][0]["Key"]



##rename the object

client.delete_object(Bucket=BUCKET_NAME, Key=currentdate+'_dim_amazon_product.csv')


copy_source = {'Bucket': BUCKET_NAME, 'Key': name}




copy_key =  currentdate+'_dim_amazon_product.csv'

## Rename and copy into same bucket

client.copy(CopySource=copy_source, Bucket=BUCKET_NAME, Key=copy_key)

## copy into archive

client.copy(CopySource=copy_source, Bucket=BACKUP_BUCKET_NAME, Key=copy_key)

## deletes spark object 

client.delete_object(Bucket=BUCKET_NAME, Key=name)
```

# Code snippet for loading into 3 tables into redshift 

1) FCT_AMAZON_REVIEWS

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sc2 = SparkSession.builder.appName("redshift_fct_load").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g") .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

##@params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##Source Files to Load into redshift

## @type: DataSource
## @args: [database = "reviews", table_name = "justeatconsumption", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
fct_amz_review = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "justeatconsumption", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")



## @type: DataSink
## @args: [catalog_connection = "redshift", connection_options = {"database" : "d0_l1_edw", "dbtable" : "fct_amazon_reviews"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: datasink4
## @inputs: [frame = fct_amz_review]


##Writing output to redshift Bucket

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = fct_amz_review, catalog_connection = "redshift", connection_options = {"dbtable": "fct_amazon_reviews", "database": "d0_l1_edw"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")


job.commit()

```
2) DIM_REVIEWS

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sc2 = SparkSession.builder.appName("redshift_dim_review_load").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g") .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

##@params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##Source Files to Load into redshift


## @type: DataSource
## @args: [database = "reviews", table_name = "reviewsdimconsumption", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
src_dim_reviews = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "reviewsdimconsumption", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")



## @type: DataSink
## @args: [catalog_connection = "redshift", connection_options = {"database" : "d0_l1_edw", "dbtable" : "dim_reviews"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: datasink4
## @inputs: [frame = src_dim_reviews]


##Writing output to redshift Bucket

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = src_dim_reviews, catalog_connection = "redshift", connection_options = {"dbtable": "dim_reviews", "database": "d0_l1_edw"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")


job.commit()

```
3) DIM_AMAZON_PRODUCT

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sc2 = SparkSession.builder.appName("redshift_dim_product_load").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g") .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

##@params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##Source Files to Load into redshift


## @type: DataSource
## @args: [database = "reviews", table_name = "metadatadimconsumption", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
src_dim_amazon_product = glueContext.create_dynamic_frame.from_catalog(database = "reviews", table_name = "metadatadimconsumption", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>")



## @type: DataSink
## @args: [catalog_connection = "redshift", connection_options = {"database" : "d0_l1_edw", "dbtable" : "dim_amazon_product"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: datasink4
## @inputs: [frame = src_dim_amazon_product]


##Writing output to redshift Bucket

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = src_dim_amazon_product, catalog_connection = "redshift", connection_options = {"dbtable": "dim_amazon_product", "database": "d0_l1_edw"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")


job.commit()

```


# Below is the BOT Code to cleanup the s3 bucket before each load.

import boto3    

s3 = boto3.resource('s3')

bucket = s3.Bucket('<bucket_name>')

bucket.objects.all().delete()

```
