package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.io.Source

object master {

	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

			val  df1 = spark.read.format("parquet").load("s3://datastreamcorp/dest/sitecount")

			val  df2 = spark.read.format("parquet").load("s3://datastreamcorp/dest/total_amount_data")
			
			val  df3 = spark.read.format("parquet").load("s3://datastreamcorp/dest/customer_api")
			
			val rm=df3.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
			
			val finaljoin = rm.join(df1,Seq("username"),"left").join(df2,Seq("username"),"left")
			
			finaljoin.drop("cnt").write.format("parquet").mode("overwrite").save("s3://datastreamcorp/dest/finalcustomer")
				
			

	}

}
