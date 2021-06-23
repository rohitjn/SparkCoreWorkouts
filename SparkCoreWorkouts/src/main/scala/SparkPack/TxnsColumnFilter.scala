package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf

object TxnsColumnFilter {

	//Define case class  (after object before main method)
	//case class columns(category:String,product:String,city:String,state:String,spendby:String) 
case class columns(category:String,product:String,city:String,state:String,spendby:String)

def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
				val sc = new SparkContext(conf);

		sc.setLogLevel("ERROR");



		//Read txns file 
		println("============= RAW DATA ==================")
		val data = sc.textFile("file:///D:/data/txns.csv")
		data.take(5).foreach(println)

		//Do map split with , (comma)
		println("============= Map Split data DATA ==================")
		val map_split_data = data.map(x=>x.split(","))
		map_split_data.take(5).foreach(println)

		//While imposing the schema ===val colrdd= mapsplit.map(columns(x(4),x(5),x(6),x(7),x(8)))
		println("============= Imposed DATA ==================")

		val imposed_data = map_split_data.map(x=>columns(x(4),x(5),x(6),x(7),x(8)))
		imposed_data.take(5).foreach(println)			

		//Filter spendby=cash
		println("============= Filter DATA ==================")

		val fildata = imposed_data.filter(x=>x.spendby.contains("cash"))
		fildata.take(5).foreach(println)			

}
}