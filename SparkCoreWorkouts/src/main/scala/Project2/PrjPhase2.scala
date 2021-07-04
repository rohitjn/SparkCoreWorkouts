package Project2

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import org.apache.spark.broadcast._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.io.Source._
import org.apache.spark.sql.hive._
import org.apache.spark.api.java.JavaSparkContext

object PrjPhase2 {
	def main(args: Array[String]): Unit = {

			//	Phase 1
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

			// Phase 2
			//1)	Create a folder in windows with yesterday's (2021-07-03) and place your avro file
			//2)	Programmatically find yesterday's date save it in a variable, send that variable to path while reading the data
			//	val date="2021-07-03"
			//	val df = spark.read.format(avro).load(s"file:///C://data//$date")
			//	check whether the data is getting read
			//3)	Create a table in hive  (See column names from announcement) having reference as joined dataframe -- External Parquet
			//4)	Have joined dataframe from phase 1 ---- Generated Index column using the method consist of zipwith index
			//5)	Replace id column with index column
			//6)	Go to spark shell Get the max id value from hive table created (retuns null)  Replace it with 0 using coalesce
			//7)	U will get 0 in the dataframe - convert it to string and then to .toInt
			//8)	Copy this executed code in your eclipse project -- ensure (hive context is imported after spark session)
			//9)	Add that max value variable to the generated index at  step 5
			//10)	Resultant dataframe would be written hive table  (saveAsTable)
			//11)	Create a folder in hdfs  /user/cloudera/2021-07-03 --- have avro file
			//12)	Change the source path to
			//val df = spark.read.format(avro).load(s"hdfs:/user/cloudera/$date")
			//,then  Export it as jar-- Deploy it


			val sc = new SparkContext(new SparkConf().setAppName("RJ").setMaster("local[*]")).setLogLevel("ERROR")
//			val javasc = new JavaSparkContext()
//					val hiveContext = new HiveContext(javasc)
//					import hiveContext.implicits._

					val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._


			println("############1)	Create a folder in windows with yesterday's (2021-07-03) and place your avro file (D:\\data\\project-data\\2021-07-23) ")
			val filePath = "D://data//project-data//"

			println("############2)	Programmatically find yesterday's date save it in a variable, send that variable to path while reading the data ")
			val currdate = current_date().expr;
			val yesterday = java.time.LocalDate.now.minusDays(1)
					println("current date: " + currdate+" "+yesterday)

					//      println("	val date="2021-07-03"")
					//      println("	val df = spark.read.format(avro).load(s"file:///C://data//$date")")
					println("############ 2.	Read the avro file  and have a dataframe")
					println("	check whether the data is getting read ")
					// windows
//					val avrodf = spark.read.format("com.databricks.spark.avro").load(s"file:///$filePath//$yesterday")
					// cloudera
					val avrodf = spark.read.format("com.databricks.spark.avro").load(s"hdfs:/user/cloudera/$yesterday")
					avrodf.show()
					avrodf.printSchema()

					println("3)	Create a table in hive  (See column names from announcement) having reference as joined dataframe -- External Parquet") 

					println("############ 3.	Read url data https://randomuser.me/api/0.8/?results=20")

					val url = "https://randomuser.me/api/0.8/?results=200"

					println("\n ------------- response --------------- ")
					val apiResponse = fromURL(url).mkString
					//			println(apiResponse)

					val rdd = spark.sparkContext.parallelize(List(apiResponse))
					//			rdd.take(5).foreach(println)
					val urlResponseDf = spark.read.option("inferSchema", "true").json(rdd)
					urlResponseDf.show(2)
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
							col("results.user.dob"),
							col("results.user.email"),
							col("results.user.location.city"),
							col("results.user.location.state"),
							col("results.user.location.street"),
							col("results.user.location.zip"),
							col("results.user.name.first"),
							col("results.user.name.last"),
							col("results.user.name.title"),
							col("results.user.registered"),
							col("results.user.username"))

					println("############ 5.	Remove numericals from username from that dataframe  (Regex replace)")
					val userCleanedDf = urlFlatdf.withColumn("username", regexp_replace(col("username"), "\\d", ""))
					userCleanedDf.show(5)

					println("############ 6.	Join step2 dataframe with step 5 dataframe with broadcast left on username column")
					//			val joindf = avrodf.join(broadcast(userCleanedDf), avrodf("username")===userCleanedDf("username1") , "inner").drop(col)
					val joindf = avrodf.join(broadcast(userCleanedDf), Seq("username") , "left")
					joindf.show(5)

					println("############ 4)	Have joined dataframe from phase 1 ---- Generated Index column using the method consist of zipwith index")
					val dfWithIndex =  addColumnIndex(spark, joindf)   
					dfWithIndex.show(5);    

			println("############ 5) Replace id column with index column")
			val dfwithoutId = dfWithIndex.withColumn("id", col("index"))
			val dfDropIndex = dfwithoutId.drop("index")

			dfDropIndex.show(5);    

			
			println("############ 6) Go to spark shell Get the max id value from hive table created (retuns null)  Replace it with 0 using coalesce")
//			val maxDf = spark.sql("select nvl(max(id),0) from prj2_spark_hive_data")

      println("############ 7) U will get 0 in the dataframe - convert it to string and then to .toInt")
//			val maxvalue=maxDf.rdd.map(x=>x.mkString("")).collect().mkString("").toInt
			 val maxvalue = 5
			 
	    println("############ 8) Copy this executed code in your eclipse project -- ensure (hive context is imported after spark session)")
    println("############ 9) Add that max value variable to the generated index at  step 5")
		val dfAddToId = dfDropIndex.withColumn("id", col("id")+maxvalue)

		dfAddToId.show(2)

    println("############ 10)  Resultant dataframe would be written hive table  (saveAsTable)")
    
    dfAddToId.write.format("hive").mode("append").saveAsTable("sparkprj2.prj2_spark_hive_data")
    
    println("############ 11)  Create a folder in hdfs  /user/cloudera/2021-07-03 --- have avro file ")
    
    println("############ 12)  Change the source path to as per cloudera")
    //      println("val df = spark.read.format(avro).load(s"hdfs:/user/cloudera/$date")  ")
	}


	def addColumnIndex(spark: SparkSession,df: DataFrame) = {
			spark.sqlContext.createDataFrame(
					df.rdd.zipWithIndex.map {
					case (row, index) => Row.fromSeq(row.toSeq :+ index)
					},
					// Create schema for index column
					StructType(df.schema.fields :+ StructField("index", LongType, false)))
	}
}