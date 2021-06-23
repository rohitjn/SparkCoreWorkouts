package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object USTxnsSeamlessCSVRead {
	//1.	Read usdata.csv using header True 
	//2.	printSchema--- It would be string for the all columns 
	//3.	Check which option to enable in spark.read.format.option() to auto inferSchema 

	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Start").setMaster("local[*]")
					val sc = new SparkContext(conf);
			val spark = SparkSession.builder().getOrCreate()
					sc.setLogLevel("ERROR");

			val df_csv = spark.read.option("header",true)
					.option("inferSchema",true).format("csv")
					.load("file:///D:/data/usdata.csv")
					println("\n============== Show CSV Infer Schema ============================")
					df_csv.printSchema()
					println("\n============== Show CSV Seamless DataFrame ============================")
					df_csv.show(2,false)

					println("============== write avro DF ============================")
					df_csv.write.format("com.databricks.spark.avro").save("file:///D:/data-output/usdata-avro")

	}
}