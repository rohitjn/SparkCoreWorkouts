package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf

object USTxnSplit {
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
			val sc = new SparkContext(conf);

			sc.setLogLevel("ERROR");

			
			println("============= RAW DATA ==================")
			val data = sc.textFile("file:///D:/data/usdata.csv")
			data.take(5).foreach(println)
			
			println("============= LEN DATA ==================")
			val len_data = data.filter(x=>x.length() > 200)
			len_data.take(5).foreach(println)

			println("============= Flat DATA ==================")			
			val flat_data = len_data.flatMap(x=>x.split(","))
			flat_data.take(5).foreach(println)
			
		
			println("============= Replace DATA ==================")
			val replace_data = flat_data.map(x=>x.replace("\"",""))
			replace_data.take(5).foreach(println)
			
			println("============= Map DATA ==================")
			val add_data = flat_data.map(x=>x.replace("\"",""))
			replace_data.take(5).foreach(println)
			
			println("============= RAW DATA ==================")

			println("============= data written ==================")
			flat_data.saveAsTextFile("file:///D:/data-output/usdata-split-comma")

	}
}