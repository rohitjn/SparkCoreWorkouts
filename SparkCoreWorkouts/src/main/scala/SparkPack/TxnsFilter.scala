package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf

object TxnsFilter {
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf);

			sc.setLogLevel("DEBUG");

			val data = sc.textFile("file:///D:/data/txns")
					val fildata = data.filter(x=>x.contains("Gymnastics"))
//					fildata.saveAsTextFile("file:///D:/data-output/gymdata_dir_zeyo12")
					fildata.saveAsTextFile("file:///D:/data-output/txns_gymdata")

	}
}