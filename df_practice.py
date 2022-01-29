from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DecimalType

DF = spark.read.option("sep","|").option("header","True").format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.dat")
l = DF.columns
print(len(l))

#select usingn string object
#customerWithBirthdayDF1 = DF.select("c_customer_id","c_first_name","c_last_name","c_birth_year","c_birth_month","c_birth_day")
#customerWithBirthdayDF.show()

#select using column object
customerWithBirthdayDF2 = DF.select(col("c_customer_id"),col("c_first_name"),col("c_last_name"),col("c_birth_year"),col("c_birth_month"),column("c_birth_day"))
customerWithBirthdayDF2.show()
#infer schema for customer_address dataset

customerAddressDF  = spark.read.option("inferSchema","True").option("header","True").option("sep","|").csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
customerAddressDF.printSchema()
customerAddressDF.show()


# defining own schema for customer_address dataset

addressSchema = StructType([
                StructField("ca_address_sk", IntegerType(),False),     
                StructField("ca_address_id",StringType(),True),
                StructField("ca_street_number",StringType(),True),
                StructField("ca_street_type",StringType(),True),
                StructField("ca_suite_number",StringType(),True),
                StructField("ca_city",StringType(),True),
                StructField("ca_county",StringType(),True),
                StructField("ca_state",StringType(),True),
                StructField("ca_zip",StringType(),True),
                StructField("ca_country",StringType(),True),
                StructField("ca_gmt_offset",DecimalType(),True),
                StructField("ca_location_type",StringType(),True) ])
customerAddressDF1  = spark.read.schema(addressSchema).option("header","True").option("sep","|").csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
customerAddressDF1.printSchema()
customerAddressDF1.show()
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DecimalType

DF = spark.read.option("sep","|").option("header","True").format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.dat")
l = DF.columns
print(len(l))

#select usingn string object
#customerWithBirthdayDF1 = DF.select("c_customer_id","c_first_name","c_last_name","c_birth_year","c_birth_month","c_birth_day")
#customerWithBirthdayDF.show()

#select using column object
customerWithBirthdayDF2 = DF.select(col("c_customer_id"),col("c_first_name"),col("c_last_name"),col("c_birth_year"),col("c_birth_month"),column("c_birth_day"))
customerWithBirthdayDF2.show()
#infer schema for customer_address dataset

customerAddressDF  = spark.read.option("inferSchema","True").option("header","True").option("sep","|").csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
customerAddressDF.printSchema()
customerAddressDF.show()


# defining own schema for customer_address dataset

addressSchema = StructType([
                StructField("ca_address_sk", IntegerType(),False),     
                StructField("ca_address_id",StringType(),True),
                StructField("ca_street_number",StringType(),True),
                StructField("ca_street_type",StringType(),True),
                StructField("ca_suite_number",StringType(),True),
                StructField("ca_city",StringType(),True),
                StructField("ca_county",StringType(),True),
                StructField("ca_state",StringType(),True),
                StructField("ca_zip",StringType(),True),
                StructField("ca_country",StringType(),True),
                StructField("ca_gmt_offset",DecimalType(),True),
                StructField("ca_location_type",StringType(),True) ])
customerAddressDF1  = spark.read.schema(addressSchema).option("header","True").option("sep","|").csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
customerAddressDF1.printSchema()
customerAddressDF1.show()

#adding column to income_band DF
from pyspark.sql.functions import *

incomeband_schema = StructType([
                    StructField("ib_income_band_sk", LongType(),False),     
                    StructField("ib_lower_bound",IntegerType(),True),
                    StructField("ib_upper_bound",IntegerType(),True),
                ])
incomebandDF= spark.read.schema(incomeband_schema).option("header",'True').csv("/FileStore/tables/retailer/data/income_band.csv")
incomeDfWithGroups = incomebandDF.withColumn("isFirstIncomeGroup", column("ib_upper_bound") <= 60000).withColumn("isSecondGroup",column("ib_upper_bound") >60000)
incomeDfWithGroups.show()

#filtering customer dataframe
customerDF  = spark.read.option("header","True").option("inferSchema","True").option("sep","|").csv("/FileStore/tables/retailer/data/customer.dat")
filteredDF  = customerDF.filter((col("c_birth_day") > 0) &  (col("c_birth_day") <= 31)) \
              .filter((col("c_birth_month") > 0 ) & (col("c_birth_month") < 12)) \
              .where(col("c_birth_year")>0)


customerDF.count() #100000
fileredDF.count() #96539

#joins in DataFrame
customerDF = spark.read.option("header","True").option("inferSchema","True").option("sep","|").csv("/FileStore/tables/retailer/data/customer.dat")
addressDF = spark.read.option("inferSchema","True").option("header","True").option("sep","|").csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
customerDF.printSchema()
addressDF.printSchema()

joinedDF = customerDF.join(addressDF, customerDF.c_current_addr_sk == addressDF.ca_address_sk)
display(joinedDF)
DF1 = df.select(col("lname").alias("first_name"), col("lname").alias("last_name"),col("id"),col("gender"))
# BETWEEN function
DF2 = DF1.filter(col("id").between(100,400))
DF2.show()

#ISNULL and ISNOTNULL 
DF3 = df.filter(col("gender").isNull())
DF3.show()
DF4 = df.filter(col("gender").isNotNull())
DF4.show()

#when() and otherwise()
DF5 = df.select(col("fname"),col("lname"),when(col("gender")=="M","male").\
                when(col("gender")=="F","female").\
                when(col("gender")=="","none").otherwise(col("gender")).alias("new_gender"))
DF5.show()




