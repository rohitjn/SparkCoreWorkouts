package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf

object ColumnDataFilter {
	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
					val sc = new SparkContext(conf);

			sc.setLogLevel("ERROR");

			//			read usdata.csv
			println("============= RAW DATA ==================")
			val data = sc.textFile("file:///D:/data/usdata.csv")
			data.take(5).foreach(println)

			//Filter states with LA (x=>x.contains(",LA"))
			println("============= Column DATA ==================")
			val map_data = data.map(x=>x.split(","))
			map_data.take(5).foreach(println)
			
			
			val column_data = map_data.map(x=>(x(0)))
			column_data.take(5).foreach(println)			
	
	}
}