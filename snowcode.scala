package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object snow {

	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._



			import sys.env

			val snowdf = spark.read
			  .format("snowflake")
			  .option("sfURL", sys.env("SF_URL"))
			  .option("sfAccount", sys.env("SF_ACCOUNT"))
			  .option("sfUser", sys.env("SF_USER"))
			  .option("sfPassword", sys.env("SF_PASSWORD"))
			  .option("sfDatabase", sys.env("SF_DATABASE"))
			  .option("sfSchema", sys.env("SF_SCHEMA"))
			  .option("sfRole", sys.env("SF_ROLE"))
			  .option("sfWarehouse", sys.env("SF_WAREHOUSE"))
			  .option("dbtable", sys.env("SF_DBTABLE"))
			  .load()


			val aggdf = snowdf.groupBy("username").agg(count("site").as("cnt"))



			aggdf.write.format("parquet").mode("overwrite").save("s3://datastreamcorp/dest/sitecount")
















	}
}
