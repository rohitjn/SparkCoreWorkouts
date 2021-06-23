package SparkPack

object StateCity {
  def main(args: Array[String]): Unit = {
    val data_str= List(

					"State->Andhra~City->Vijayawada",
					"State->TamilNadu~City->Chennai",
					"State->Maharashtra~City->Mumbai"

					)
					
			val flat_data = data_str.flatMap(x=>x.split("~"))
					flat_data.foreach(println)

			val state = 	flat_data.filter(x=>x.contains("State"))
			val finalstate = state.map(x=>x.replace("State->", ""))
			
			
			val city = 	flat_data.filter(x=>x.contains("city"))
			val finalcity = state.map(x=>x.replace("City->", ""))
			
			finalstate.foreach(println)
			println
			finalcity.foreach(println)
			
  }
}