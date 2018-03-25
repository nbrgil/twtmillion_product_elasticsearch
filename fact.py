from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType


class Fact:
	def __init__(self, spark_session):
		self.spark = spark_session
		self.sc = self.spark.sparkContext
	
	def csv_to_tmp_view(self, view_name="fact"):
		schema = StructType([
			StructField("index", IntegerType(), True),
			StructField("id", IntegerType(), True),
			StructField("order_date", StringType(), True),
			StructField("view_date", StringType(), True),
			StructField("currency", IntegerType(), True),
			StructField("device", IntegerType(), True),
			StructField("product", StringType(), True),
			StructField("channel", StringType(), True),
			StructField("store", IntegerType(), True),
			StructField("company", IntegerType(), True),
			StructField("full_view_date", TimestampType(), True),
			StructField("gross_total_volume", FloatType(), True),
			StructField("product_net_cost", FloatType(), True),
			StructField("product_net_revenue", FloatType(), True),
			StructField("gross_merchandise_volume", FloatType(), True),
			StructField("item_sold", FloatType(), True),
			StructField("page_views", IntegerType(), True)]
		)
		
		df_fact = self.spark.read.format("csv").option("header", "true").schema(schema).csv("data/fact.csv")
		df_fact.createOrReplaceTempView("fact")