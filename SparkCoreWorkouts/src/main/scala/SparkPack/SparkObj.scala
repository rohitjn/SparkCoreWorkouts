package SparkPack

object SparkObj {
	def main(args: Array[String]): Unit = {
			/*
	    println ("============ raw data============")
	    val list_int = List (1,2,3,4,80,5,6,7,8)
			list_int.foreach(println)
			println;

	    println ("============ filtered data============")

			val filter_list = list_int.filter(x => x>4);		
					filter_list.foreach(println)
			 	    println ("============ Multiply data============")

			val mapdata = list_int.map(x => x*4);		
					mapdata.foreach(println)
			 */			

			/*			val list_str = List("zeyobron","zeyo","analytics","Sai","zeyobrona")

					list_str.foreach(println)

					println("====================Filter zeyo=============")
					println

					val fil_list = list_str.filter(x=>x.contains("zeyo"))

					fil_list.foreach(println)

					println("====================Concat rohit=============")
					println

					val mapdata = list_str.map(x=>x + " rohit")

					mapdata.foreach(println)		
			 */				

			val list_str = List("Sai,Aditya",
					"Hema,Srinivas",
					"Sai,Chaitanya",
					"Hajira,Begum")
					list_str.foreach(println)               

					// I want each element to be flattened with respect to comma split delimiter 


					// Map will just do it will not flatten
					println
					println("================Flattened data===============")

					val flat_data = list_str.flatMap(x=>x.split(","))
					flat_data.foreach(println)

					println("================map replace===============")
					val map_replace = flat_data.map(x=>x.replace("Sai", "NA"))

					map_replace.foreach(println)

	}
}