package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object TxnsDfRowRDDTask3 {
/*
  1.	Read txns
  2.	Mapsplit
  3.	Convert that to Row rdd
  4.	Define struct type 
  import org.apache.spark.sql.types._
  val schema_struct = StructType(Array(
  	StructField("txnno",StringType,true),
  	StructField("txndate",StringType,true),
  	StructField("custno",StringType,true),
  	StructField("amount", StringType, true),
  	StructField("category", StringType, true),
  	StructField("product", StringType, true),
  	StructField("city", StringType, true),
  	StructField("state", StringType, true),
  	StructField("spendby", StringType, true)
  	))
  5.	Create a dataframe 
  val rowdf = spark.createDataFrame(rowrdd, schema_struct)
  6.	Shoot a sql query Filter Product like Gymnastics and txnno>40000 
*/
  
	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
					val sc = new SparkContext(conf);
			val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					sc.setLogLevel("ERROR");

			println("\n============= RAW DATA ==================")
			val data = sc.textFile("file:///D:/data/txns.csv")
			data.take(5).foreach(println)

			println("\n============= map_split_data DATA ==================")
			val map_split_data = data.map(x=>x.split(","))
			map_split_data.take(5).foreach(println)

			println("\n============= row_rdd_data DATA ====================")
			val row_rdd_data = map_split_data.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
			row_rdd_data.take(5).foreach(println)

			val schema_struct = StructType(Array(
					StructField("txnno",StringType,true),
					StructField("txndate",StringType,true),
					StructField("custno",StringType,true),
					StructField("amount", StringType, true),
					StructField("category", StringType, true),
					StructField("product", StringType, true),
					StructField("city", StringType, true),
					StructField("state", StringType, true),
					StructField("spendby", StringType, true)
					))

			val df_row_rdd = spark.createDataFrame(row_rdd_data, schema_struct)
			println ("\n============ DataFrame Data ==============")
			df_row_rdd.show()		

			df_row_rdd.createOrReplaceTempView("t_txns")

			println ("\n============ GYM Data ==============")
			val data_gym = spark.sql("select * from t_txns where product like '%Gymnastics%' and txnno>40000 ") 
			data_gym.show()

	}

}