from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, regexp_replace


class ProductDimension:
	"""
		Reads dim.csv and
	"""

	def __init__(self, spark_session):
		self.spark = spark_session
		self.sc = self.spark.sparkContext
	
	def file_to_df(self):
		dim_lines = self.sc.textFile("data/dim.csv")
		header = dim_lines.first()
		dim_lines = dim_lines.filter(lambda r: r != header)
		dim_parts = dim_lines.map(lambda l: l.split(","))
		
		dim = dim_parts.map(lambda p: Row(
			is_banner=p[1],
			is_tax=p[2],
			is_market_place=p[3],
			material_status=p[4],
			current_price_range=p[5],
			cmc_division=p[6],
			cmc_business_unit=p[7],
			gender=p[8],
			product=p[9]
		))
		
		return self.spark.createDataFrame(dim)
	
	@staticmethod
	def gender_transform(df):
		gender = expr("UPPER(CASE WHEN gender IS NULL OR gender = '' THEN 'UNISSEX' ELSE gender END)")
		df = df.withColumn("gender", gender)
		df = df.withColumn("gender", regexp_replace(df['gender'], '(.)*(UNISEX)(.)*', 'UNISSEX'))
		df = df.withColumn("gender", regexp_replace(df['gender'], '(.)*(MASC){1}(.)*(FEM){1}(.)*', 'UNISSEX'))
		df = df.withColumn("gender", regexp_replace(df['gender'], '(.)*(FEM){1}(.)*(MASC){1}(.)*', 'UNISSEX'))
		df = df.withColumn("gender", regexp_replace(df['gender'], '(.)*(BEB)(.)*', 'BEBE'))
		df = df.withColumn(
			"gender", regexp_replace(
				df['gender'], '(.)*(NIÑ|MENIN|BOYS|GIRL|INFANTIL|JUVENIL)(.)*', 'INFANTIL'
			)
		)
		df = df.withColumn("gender", regexp_replace(df['gender'], '(.)*(FEM)(.)*', 'FEMININO'))
		df = df.withColumn("gender", regexp_replace(df['gender'], '(.)*(MASC)(.)*', 'MASCULINO'))
		
		return df
	
	def csv_to_tmp_view(self, view_name="dim"):
		df = self.file_to_df()
		df = self.gender_transform(df)
		
		df.createOrReplaceTempView(view_name)