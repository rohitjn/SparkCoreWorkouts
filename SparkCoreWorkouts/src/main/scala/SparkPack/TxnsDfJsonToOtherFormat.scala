package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TxnsDfJsonToOtherFormat {

  
	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
					val sc = new SparkContext(conf);
			val spark = SparkSession.builder().getOrCreate()
					sc.setLogLevel("ERROR");

		val df_json = spark.read.format("json").load("file:///D:/data/devices.json")
				println("============== Show JSON DF ============================")
				df_json.printSchema()
				df_json.show(2,false)
//
//								println("============== Write parquet DF ============================")
//				df_json.write.format("parquet").mode("error").save("file:///D:/data-output/devices-parquet")
//				
								println("============== write avro DF ============================")
				df_json.write.format("com.databricks.spark.avro").save("file:///D:/data-output/devices-avro")

	}

}