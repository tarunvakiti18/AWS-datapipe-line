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



			val snowdf = spark.read.format("snowflake").option("sfURL","https://hgffczj-aj09505.snowflakecomputing.com").option("sfAccount","hgffczj").option("sfUser","sivavasusaia").option("sfPassword","Aditya908").option("sfDatabase","zeyodb").option("sfSchema","zeyoschema").option("sfRole","ACCOUNTADMIN").option("sfWarehouse","COMPUTE_WH").option("dbtable","srctab").load()


			val aggdf = snowdf.groupBy("username").agg(count("site").as("cnt"))



			aggdf.write.format("parquet").mode("overwrite").save("s3://datastreamcorp/dest/sitecount")
















	}
}