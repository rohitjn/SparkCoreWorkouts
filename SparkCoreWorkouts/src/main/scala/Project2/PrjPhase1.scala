package Project2

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Nvl
import org.apache.spark.sql.types.DecimalType

object PrjPhase1 {
	def main(args: Array[String]): Unit = {

			//1.	Spark Context,SparkSession  Initialization and have avro data in a directory in windows 
			//2.	Read the avro file  and have a dataframe
			//3.	Read url data https://randomuser.me/api/0.8/?results=200
			//4.	Flatten it completely but ignore on column which comes after cell(take care of it)
			//5.	Remove numericals from username from that dataframe  (Regex replace)
			//6.	Join step2 dataframe with step 5 dataframe with broadcast left on username column
			//7.	Create two dataframe from joined df
			//    a.	7a) Nationality is Not null (available)
			//    b.	7b) nationality is null  (non available)
			//8.	Take  non available customer dataframe replace nulls string with NA null non strings columns with 0

			println("############ 1.	Spark Context,SparkSession  Initialization and have avro data in a directory in windows") 

			val sc = new SparkContext(new SparkConf().setAppName("RJ").setMaster("local[*]")).setLogLevel("ERROR")

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._

			println("############ 2.	Read the avro file  and have a dataframe")
			val avrodf = spark.read.format("com.databricks.spark.avro").load("file:///D://data//project-data//part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro")
			avrodf.show()
			avrodf.printSchema()

			println("############ 3.	Read url data https://randomuser.me/api/0.8/?results=200")

			val url = "https://randomuser.me/api/0.8/?results=200"

			println("\n ------------- response --------------- ")
			val apiResponse = fromURL(url).mkString
			println(apiResponse)
			val rdd = spark.sparkContext.parallelize(List(apiResponse))
			rdd.take(5).foreach(println)
			val urlResponseDf = spark.read.option("inferSchema", "true").json(rdd)
			urlResponseDf.show()
			urlResponseDf.printSchema()			

			println("############ 4.	Flatten it completely but ignore on column which comes after cell(take care of it)")
			val urlResPartialexplodedf = urlResponseDf.select(
					col("nationality"),
					col("seed"),
					col("version"),
					col("results")
					).withColumn("results", explode(col("results")))

			urlResPartialexplodedf.show(true)
			//			urlResPartialexplodedf.printSchema()

			val urlFlatdf = urlResPartialexplodedf.select(
					col("nationality"),
					//							col("seed"),
					//							col("version"),
					//	col("results"),
					//	col("results.user.AVS"),
					//							col("results.user.cell"),
					col("results.user.dob"),
					col("results.user.email"),
					col("results.user.location.city"),
					col("results.user.location.state"),
					col("results.user.location.street"),
					col("results.user.location.zip"),
					//							col("results.user.md5"),
					col("results.user.name.first"),
					col("results.user.name.last"),
					col("results.user.name.title"),
					//							col("results.user.password"),
					//							col("results.user.phone"),
					//							col("results.user.picture.large"),
					//							col("results.user.picture.medium"),
					//							col("results.user.picture.thumbnail"),
					col("results.user.registered"),
					//							col("results.user.salt"),
					//							col("results.user.sha1"),
					//							col("results.user.sha256"),
					col("results.user.username"))

			//					urlFlatdf.show()
			//					urlFlatdf.printSchema()

			println("############ 5.	Remove numericals from username from that dataframe  (Regex replace)")
			val userCleanedDf = urlFlatdf.withColumn("username", regexp_replace(col("username"), "\\d", ""))
			userCleanedDf.show()

			println("############ 6.	Join step2 dataframe with step 5 dataframe with broadcast left on username column")
			//			val joindf = avrodf.join(broadcast(userCleanedDf), avrodf("username")===userCleanedDf("username1") , "inner").drop(col)
			val joindf = avrodf.join(broadcast(userCleanedDf), Seq("username") , "left")
			joindf.show()

			println("############ 7.	Create two dataframe from joined df")

			println("############     a.	7a) Nationality is Not null (available)")
			val joinNationalDf = joindf.filter(col("nationality").isNotNull)
			joinNationalDf.show(2)

			println("############     b.	7b) nationality is null  (non available)")
			val joinNotNationalDf = joindf.filter(col("nationality").isNull)
			joinNotNationalDf.show(2)

			println("############ 8.	Take  non available customer dataframe replace nulls string with NA null non strings columns with 0")
			//			 val joinNotNationalDf1 = joinNotNationalDf.withColumn("username", expr("NVL(username,\"NA\")")).withColumn("ip", expr("NVL(ip,\"NA\")")).withColumn("amount", expr("NVL(amount,0)"))
			val replaceNullDf = joinNotNationalDf.na.fill("Not Available").na.fill(0)
			replaceNullDf.show()

			println("############ 9.	Add extra column today for both the dataframe  have current date to it ")
			val nationalDateDf =  joinNationalDf.withColumn("currentdate", current_date())			
			val nonNationalDf = replaceNullDf.withColumn("currentdate", current_date())

			nationalDateDf.show(2)
			nonNationalDf.show(2)

			println("############ 10.	write below transformation as a json file")
			val nationalAggDf = nationalDateDf.groupBy("username")
			.agg(collect_list("ip").alias("ip_list"),
					collect_list("id").alias("id_list"),
					sum(col("amount").cast(DecimalType(15,2))).alias("total_amount"),
					struct(
							count("ip").alias("ip_count"),
							count("id").alias("id_count")
							).alias("count_ip_id"))

			val nonNationalAggDf = nonNationalDf.groupBy("username")
			.agg(collect_list("ip").alias("ip_list"),
					collect_list("id").alias("id_list"),
					sum(col("amount").cast(DecimalType(15,2))).alias("total_amount"),
					struct(
							count("ip").alias("ip_count"),
							count("id").alias("id_count")
							).alias("count_ip_id"))

			nationalAggDf.show()
			nonNationalAggDf.show()

			nationalAggDf.printSchema()
			nonNationalAggDf.printSchema()
			
			nationalAggDf.write.option("multiLine", true).mode("overwrite").json("file:///D://data-output//project2-national-json")
			println("############ Finished Writing Available Data")

			nonNationalAggDf.write.option("multiLine", true).mode("overwrite").json("file:///D://data-output//project2-nonNational-json")
			println("############ Finished Writing Not Available Data")
	}
}