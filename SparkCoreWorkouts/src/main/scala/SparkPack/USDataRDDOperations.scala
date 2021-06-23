package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.SparkConf

object USDataaRDDOPerations {
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
			val sc = new SparkContext(conf);

			sc.setLogLevel("ERROR");

			
//			read usdata.csv
			println("============= RAW DATA ==================")
			val data = sc.textFile("file:///D:/data/usdata.csv")
			data.take(5).foreach(println)

//Filter states with LA (x=>x.contains(",LA"))
			println("============= LEN DATA ==================")
			val filter_data = data.filter(x=>x.contains(",LA"))
			filter_data.take(5).foreach(println)

//Replace each row  , with ~
			println("============= Replace DATA ==================")
			val replace_data = filter_data.map(x=>x.replace(",","~"))
			replace_data.take(5).foreach(println)

//FlatMap with ~
			println("============= Flat DATA ==================")			
			val flat_data = replace_data.flatMap(x=>x.split("~"))
			flat_data.take(5).foreach(println)

			
//Attach zeyo, at the start and ,zeyo end  --map(x=>"zeyo,"+x+",zeyo")
			val concatenate_data = flat_data.map(x=>"zeyo,"+x+",zeyo")
			concatenate_data.take(5).foreach(println)

//Write it a directory having only 1 file
			println("============= data written ==================")
			concatenate_data.coalesce(1).saveAsTextFile("file:///D:/data-output/USDataaRDDOPerations1")

	}
}