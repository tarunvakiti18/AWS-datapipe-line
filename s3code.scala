package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.io.Source

object s3 {
  
  def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
			
			val s3df  = spark.read.format("parquet").load("s3://datastreamcorp/src")

val aggdf = s3df.groupBy("username").agg(sum("amount").cast(IntegerType).as("total"))

 aggdf.write.format("parquet").mode("overwrite").save("s3://datastreamcorp/dest/total_amount_data")

	}
  
  
}