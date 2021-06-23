package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf


object TxnsColumnFilterTask2 {

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

		//4.	Map Split txns
		println("============= Map Split data DATA ==================")
		val map_split_data = data.map(x=>x.split(","))
		map_split_data.take(5).foreach(println)

		//5.	Impose the Schema
		println("============= Imposed DATA ==================")

		val imposed_data = map_split_data.map(x=>columns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
		imposed_data.take(5).foreach(println)			

		//6.	and print 10 row ------expected - schema(all columns)
    //1.	Apply Two filter 
    //2.	.filter(x=>x.product.contains("Gymnastics") & x.spendby.contains("cash"))
    //3.	Print and show
		println("============= Filter output ==================")
		val filter_data = imposed_data.filter(x=>x.product.contains("Gymnastics") & x.spendby.contains("cash"))
		filter_data.take(10).foreach(println)

		


}
}