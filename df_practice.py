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
