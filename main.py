from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType

"""
Boilerplate - build spark
"""
spark = SparkSession.builder.appName('BrightwheelTakehome').getOrCreate()

"""
Define schema for final df
"""
schema = StructType([ \
    StructField("accepts_financial_aid",StringType(),True),
    StructField("ages_served",StringType(),True),
    StructField("capacity",IntegerType(),True),
    StructField("certificate_expiration_date",DateType(),True),
    StructField("city",StringType(),True),
    StructField("address1",StringType(),False),
    StructField("address2",StringType(),True),
    StructField("company",StringType(),True),
    StructField("phone",StringType(),False),
    StructField("phone2",StringType(),True),
    StructField("county",StringType(),True),
    StructField("curriculum_type",StringType(),True),
    StructField("email",StringType(),True),
    StructField("first_name",StringType(),True),
    StructField("language",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("license_status",StringType(),True),
    StructField("license_issued",DateType(),True),
    StructField("license_number",IntegerType(),True),
    StructField("license_renewed",DateType(),True),
    StructField("license_type",StringType(),True),
    StructField("licensee_name",StringType(),True),
    StructField("max_age",IntegerType(),True),
    StructField("min_age",IntegerType(),True),
    StructField("operator",StringType(),True),
    StructField("provider_id",StringType(),True),
    StructField("schedule",StringType(),True),
    StructField("state",StringType(),True),
    StructField("title",StringType(),True),
    StructField("website_address",StringType(),True),
    StructField("zip",StringType(),True),
    StructField("facility_type",StringType(),True),
    StructField("source",StringType(),True),
  ])

"""
Extract
"""
df1 = spark.read.csv('source1.csv',header=True)
df2 = spark.read.option("multiline", "true").csv('source2.csv',header=True)
df3 = spark.read.csv('source3.csv',header=True)


"""
Transform Source 1
"""
# rename columns
column_map = {
    "Expiration Date": "certificate_expiration_date",
    "Address": "address1",
    "Name": "company",
    "Phone": "phone",
    "County": "county",
    "Status": "license_status",
    "First Issue Date": "license_issued",
    "Credential Number": "license_number",
    "Expiration Date": "license_renewed",
    "Credential Type": "license_type",
    "State": "state",
    "Primary Contact Role": "title",
}
for key in column_map:
    df1 = df1.withColumnRenamed(key, column_map[key])

# clean and transform columns
df1 = df1.withColumn("phone", F.regexp_replace("phone", "[^0-9]", ""))
df1 = df1.withColumn("license_number", F.regexp_replace("license_number", "[^0-9]", "").cast(IntegerType()))
df1 = df1.withColumn("name_parts", F.split(F.col("Primary Contact Name"), " ")) \
         .withColumn("first_name", F.col("name_parts").getItem(0)) \
         .withColumn("last_name", F.when(F.size(F.col("name_parts")) == 2, F.col("name_parts").getItem(1))
                           .otherwise(F.lit(None))) \
         .drop("name_parts")
df1 = df1.withColumn("source", F.lit("source 1"))

# Add missing columns
existing_columns = df1.columns
for field in schema:
    if field.name not in existing_columns:
        # Add missing column with None (or default) and cast to the appropriate data type
        df1 = df1.withColumn(field.name, F.lit(None).cast(field.dataType))

# Drop Columns
schema_columns = [field.name for field in schema.fields]
columns_to_drop = [col for col in df1.columns if col not in schema_columns]
df1 = df1.drop(*columns_to_drop)


"""
Transform Source 2
"""
# rename columns
column_map = {
    "Accepts Subsidy": "accepts_financial_aid",
    "City": "city",
    "Address1": "address1",
    "Address2": "address2",
    "Company": "company",
    "Phone": "Phone",
    "Email": "email",
    "State": "state",
    "Zip": "zip",
}
for key in column_map:
    df2 = df2.withColumnRenamed(key, column_map[key])

# clean and transform columns
df2 = df2.withColumn("phone", F.regexp_replace("phone", "[^0-9]", ""))
df2 = df2.withColumn("Primary Caregiver", F.regexp_replace("Primary Caregiver", "\n", "")) \
         .withColumn("name_parts", F.split(F.col("Primary Caregiver"), " ")) \
         .withColumn("first_name", F.col("name_parts").getItem(0)) \
         .withColumn("last_name", F.col("name_parts").getItem(1)) \
         .drop("name_parts")
df2 = df2.withColumn("license_type", F.rtrim(F.split(F.col("Type License"), "-").getItem(0)))
df2 = df2.withColumn("license_number", F.split(F.col("Type License"), "-").getItem(1)) \
         .withColumn("license_number", F.regexp_replace("license_number", "[^0-9]", "").cast(IntegerType()))
df2 = df2.withColumn("schedule", 
                     F.concat(
                         F.lit("Mon: "), F.col("Mon"), F.lit(", "),
                         F.lit("Tues: "), F.col("Tues"), F.lit(", "),
                         F.lit("Wed: "), F.col("Wed"), F.lit(", "),
                         F.lit("Thurs: "), F.col("Thurs"), F.lit(", "),
                         F.lit("Friday: "), F.col("Friday"), F.lit(", "),
                         F.lit("Saturday: "), F.col("Saturday"), F.lit(", "),
                         F.lit("Sunday: "), F.col("Sunday"),
                     ))
df2 = df2.withColumn("ages_served", 
                     F.concat(
                         F.lit("AA1: "), F.coalesce(F.col("Ages Accepted 1"), F.lit("")), F.lit(", "),
                         F.lit("AA2: "), F.coalesce(F.col("AA2"), F.lit("")), F.lit(", "),
                         F.lit("AA3: "), F.coalesce(F.col("AA3"), F.lit("")), F.lit(", "),
                         F.lit("AA4: "), F.coalesce(F.col("AA4"), F.lit("")), F.lit(", "),
                     ))
df2 = df2.withColumn("ages_served",
                    F.concat(
                        F.when(
                            F.col("Ages Accepted 1").contains("Infants"),
                            "Infant: Y, "
                        ).otherwise("Infant: N, "),
                        F.when(
                            F.col("Ages Accepted 1").contains("Toddlers") | F.col("AA2").contains("Toddlers"),
                            "Toddler: Y, "
                        ).otherwise("Toddler: N, "),
                        F.when(
                            F.col("Ages Accepted 1").contains("Preschool") | F.col("AA2").contains("Preschool") | F.col("AA3").contains("Preschool"),
                            "Preschool: Y, "
                        ).otherwise("Preschool: N, "),
                        F.when(
                            F.col("Ages Accepted 1").contains("School-age") | F.col("AA2").contains("School-age") | F.col("AA3").contains("School-age") | F.col("AA4").contains("School-age"),
                            "School: Y"
                        ).otherwise("School: N"),
                    )
)
df2= df2.withColumn("source", F.lit("source 2"))

# Add missing columns
existing_columns = df2.columns
for field in schema:
    if field.name not in existing_columns:
        # Add missing column with None (or default) and cast to the appropriate data type
        df2 = df2.withColumn(field.name, F.lit(None).cast(field.dataType))

# Drop Columns
schema_columns = [field.name for field in schema.fields]
columns_to_drop = [col for col in df2.columns if col not in schema_columns]
df2 = df2.drop(*columns_to_drop)


"""
Transform Source 3
"""
# rename columns
column_map = {
    "Capacity": "capacity",
    "City": "city",
    "Address": "address1",
    "Operation name": "company",
    "Phone": "Phone",
    "County": "county",
    "Email Address": "email",
    "Satus": "license_status",
    "Issue Date": "license_issued",
    "Type": "license_type",
    "State": "state",
    "Zip": "zip",
    "Facility ID": "facility_type",
}
for key in column_map:
    df3 = df3.withColumnRenamed(key, column_map[key])

# clean and transform columns
df3 = df3.withColumn("phone", F.regexp_replace("phone", "[^0-9]", ""))
df3 = df3.withColumn("ages_served", 
                     F.concat(
                         F.lit("Infant: "), F.coalesce(F.col("Infant"), F.lit("")), F.lit(", "),
                         F.lit("Toddler: "), F.coalesce(F.col("Toddler"), F.lit("")), F.lit(", "),
                         F.lit("Preschool: "), F.coalesce(F.col("Preschool"), F.lit("")), F.lit(", "),
                         F.lit("School: "), F.coalesce(F.col("School"), F.lit("")),
                     ))
df3 = df3.withColumn("source", F.lit("source 3"))

# Add missing columns
existing_columns = df3.columns
for field in schema:
    if field.name not in existing_columns:
        # Add missing column with None (or default) and cast to the appropriate data type
        df3 = df3.withColumn(field.name, F.lit(None).cast(field.dataType))

# Drop Columns
schema_columns = [field.name for field in schema.fields]
columns_to_drop = [col for col in df3.columns if col not in schema_columns]
df3 = df3.drop(*columns_to_drop)


"""
Combine Dataframes
"""

# align column order
ordered_columns = [field.name for field in schema]
df1 = df1.select(*ordered_columns)
df2 = df2.select(*ordered_columns)
df3 = df3.select(*ordered_columns)
df = df1.union(df2).union(df3)


"""
Final Transformations and casting
"""
# Calculate max_age and min_age
df = df.withColumn("min_age",
    F.when(F.col("ages_served").contains("Infant: Y"), 0)
     .when(F.col("ages_served").contains("Toddler: Y"), 1)
     .when(F.col("ages_served").contains("Preschool: Y"), 2)
     .when(F.col("ages_served").contains("School: Y"), 5)
     .otherwise(None)
)
df = df.withColumn("max_age",
    F.when(F.col("ages_served").contains("School: Y"), 5)
     .when(F.col("ages_served").contains("Preschool: Y"), 2)
     .when(F.col("ages_served").contains("Toddler: Y"), 1)
     .when(F.col("ages_served").contains("Infant: Y"), 0)
     .otherwise(None)
)

# Cast column types
for field in schema.fields:
    column_name = field.name
    column_type = field.dataType
    if column_name in df.columns:
        df = df.withColumn(column_name, F.col(column_name).cast(column_type))


"""
Load Data
"""
url = "example/url"
table_name = "example_table"
properties = "some properties, too"
#df.write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)
