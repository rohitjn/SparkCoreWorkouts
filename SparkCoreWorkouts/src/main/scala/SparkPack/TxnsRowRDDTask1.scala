package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row


object TxnRowRDDTask1 {

	//2.	Create case class with 
	//3.	txnno STRING, txndate STRING, custno STRING, amount STRING,category STRING, product STRING, city STRING, state STRING, spendby STRING

case class columns(txnno:String, txndate:String, custno:String, amount:String, category:String,product:String,city:String,state:String,spendby:String)

def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
				val sc = new SparkContext(conf);

		sc.setLogLevel("ERROR");

		//1.	Read Txns
		println("============= RAW DATA ==================")
		val data = sc.textFile("file:///D:/data/txns.csv")
		data.take(5).foreach(println)

		//2.	Map Split txns
		println("============= Map Split data DATA ==================")
		val map_split_data = data.map(x=>x.split(","))
		map_split_data.take(5).foreach(println)


		//3.	Convert to Row rdd

		println("============= Convert to Row rdd ==================")

		val rowrdd = map_split_data.map(x=>Row(x(4),1))
		rowrdd.take(5).foreach(println)			

		//4.	Filter x(0)>40000

		println("============= Filter output ==================")
		val filter_data = rowrdd.filter(x=>x(0).toString().toInt > 40000)

				//5.	Take 10 rows and print

		filter_data.take(10).foreach(println)

}
}