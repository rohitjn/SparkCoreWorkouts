package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TxnsDfToRowRDD {

  
	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
					val sc = new SparkContext(conf);
			val spark = SparkSession.builder().getOrCreate()
					sc.setLogLevel("ERROR");

		val df_json = spark.read.format("json").load("file:///D:/data/devices.json")
				println("============== Show JSON DF ============================")
				df_json.printSchema()
				df_json.show(2,false)

		val df_orc = spark.read.format("orc").load("file:///D:/data/part_orc.orc")
				println("============== Show orc DF ============================")
				df_orc.printSchema()
				df_orc.show(2,false)

		val df_parquet = spark.read.format("parquet").load("file:///D:/data/part_par.parquet")
				println("============== Show Parquet DF ============================")
				df_parquet.printSchema()
				df_parquet.show(2,false)

	}

}