package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object USTxnsDFWriteModes {
//Read usdata.csv
//write it to directory in windows without any mode to a new directory
//Try to run the same program -- It should fail
//Try to run the same program with mode Error --- It would fail
//Try to run the same program with append Mode --- It would append the new data
//Try to run the same program with overwrite mode --- It would overwrite
//Wait for a min
//Run with ignore mode it should not write any data


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
					df_csv.write.format("com.databricks.spark.avro").mode("overwrite").save("file:///D:/data-output/usdata-avro")

	}
}