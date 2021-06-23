package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf

object USTxnSplit1 {
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
			val sc = new SparkContext(conf);

			sc.setLogLevel("DEBUG");

			val data = sc.textFile("file:///D:/data/usdata.csv")
			println("============= RAW DATA ==================")
			data.take(5).foreach(println)
					
			val flat_data = data.flatMap(x=>x.split(","))
			flat_data.take(5).foreach(println)
					
			flat_data.saveAsTextFile("file:///D:/data-output/usdata-split-comma")

	}
}