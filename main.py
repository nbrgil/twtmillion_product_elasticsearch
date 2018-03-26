from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession

from fact import Fact
from product_dimension import ProductDimension

spark = SparkSession.builder.appName("ProductApp").getOrCreate()

ProductDimension(spark_session=spark).csv_to_tmp_view()
Fact(spark_session=spark).csv_to_tmp_view()

fact_sample = spark.sql("""
	SELECT a.order_date,
		a.product,
		a.item_sold,
		b.gender,
		b.cmc_business_unit,
		a.product_net_cost,
		a.product_net_revenue,
		a.average_net_cost,
		a.absolute_margin,
		a.percentage_margin
	FROM (
		SELECT f.order_date,
			f.product,
			SUM(f.item_sold) item_sold,
			SUM(f.product_net_cost) product_net_cost,
			SUM(f.product_net_revenue) product_net_revenue,
			SUM(CASE WHEN f.item_sold > 0 THEN f.product_net_cost/f.item_sold ELSE 0 end) as average_net_cost,
			SUM(f.product_net_revenue - f.product_net_cost) as absolute_margin,
			SUM(CASE WHEN f.product_net_revenue > 0
				THEN (f.product_net_revenue - f.product_net_cost) / f.product_net_revenue * 100
				ELSE 0
			END) as percentage_margin
		FROM fact f 	
		GROUP BY f.order_date, f.product
	) a
	JOIN dim b ON (a.product = b.product)
""")

final_json = fact_sample.toJSON().take(num=20)

es = Elasticsearch()

for i, json in enumerate(final_json):
	es.index(
		index="my-index2",
		doc_type="test-type",
		id=i,
		body=json
	)