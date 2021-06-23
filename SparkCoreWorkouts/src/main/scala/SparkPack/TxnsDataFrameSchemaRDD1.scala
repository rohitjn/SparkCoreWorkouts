package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object TxnsDataFrameSchemaRDD1 {

	//2.	Create case class with 

case class columns(txnno:String, txndate:String, custno:String, amount:String, category:String,product:String,city:String,state:String,spendby:String)

def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
		val sc = new SparkContext(conf);
		val spark = SparkSession.builder().getOrCreate()
		 import spark.implicits._
		
		//1.	Read Txns
		println("============= RAW DATA ==================")
		val data = sc.textFile("file:///D:/data/txns.csv")
		data.take(5).foreach(println)

		//4.	Map Split txns
		println("============= Map Split data DATA ==================")
		val map_split_data = data.map(x=>x.split(","))
		map_split_data.take(5).foreach(println)

		//5.	Impose the Schema
		println("============= Imposed DATA ==================")

		val imposed_data = map_split_data.map(x=>columns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
		imposed_data.take(5).foreach(println)			

		
		val dataframe = imposed_data.toDF()
		
		println("=============Raw data frame=============")
				println
				
				dataframe.show()  // top 20 rows 

				println

				dataframe.createOrReplaceTempView("txndf")

				println("================gym data==============")
				println

				val gymdata = spark.sql("select * from txndf where product like '%Gymnastics%'")

				 println
				 
				 gymdata.show()

		


}
}