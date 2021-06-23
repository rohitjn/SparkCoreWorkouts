package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession

object TxnsDataFrameSchemaRDDTask2 {
	//1.	Read txns
	//2.	Mapsplit
	//3.	Convert that to schema rdd
	//4.	define case class
	//5.	Create a dataframe 
	//6.	Shoot a sql query Filter Product like Gymnastics and txnno>40000 

case class schema(txnno:String, txndate:String, custno:String, amount:String, category:String,product:String,city:String,state:String,spendby:String)

def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("FirstApp").setMaster("local[*]")
				val sc = new SparkContext(conf);
		sc.setLogLevel("ERROR");

		println("\n============= RAW DATA ==================")
		val data = sc.textFile("file:///D:/data/txns.csv")
		data.take(5).foreach(println)

		val map_split_data = data.map(x=>x.split(","))
		val schema_data = map_split_data.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
		println ("\n============ IMPOSED DATA ==============")
		schema_data.take(5).foreach(println)

		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._

		val data_frame = schema_data.toDS()
		println ("\n============ DataFrame Data ==============")
		data_frame.show()		

		data_frame.createOrReplaceTempView("t_txns")

		println ("\n============ GYM Data ==============")
		val data_gym = spark.sql("select * from t_txns where product like '%Gymnastics%' and txnno>40000 ") 
		data_gym.show()
} 


}