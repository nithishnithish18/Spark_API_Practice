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

#drop and dropDuplicates
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
print("no of rowz with duplicates "+str(df.count()))
df.show()


#PySpark distinct() function is used to drop/remove the duplicate rows (all columns) from DataFrame 
df1 = df.distinct()
print("distinct no of rows is "+ str(df1.count()))
df1.show()
'''
PySpark doesn’t have a distinct method which takes columns that should run distinct on
(drop duplicate rows on selected multiple columns) however, it provides another signature
of dropDuplicates() function which takes multiple columns to eliminate duplicates.
'''
df2 = df.dropDuplicates(["department","salary"])
print("dropDuplicate based in selected columns "+str(df2.count()))
df2.show()


#orderBy() and sort() explained
"""You can use either sort() or orderBy() function of PySpark DataFrame to sort DataFrame
by ascending or descending order based on single or multiple columns, you can also do sorting
using PySpark SQL sorting functions, In this article, I will explain all these different ways
using PySpark examples"""

#sort()
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df1 = df.sort(col("department").asc(), col("state").desc())
df1.show()

#orderby()
df2 = df.orderBy(col("department").asc(),col("state").desc())
df2.show()


#sort table usifg sparkSQL

df.createOrReplaceTempView("employee")
df3 = spark.sql("select * from employee order by department,state asc ")
df3.show()

