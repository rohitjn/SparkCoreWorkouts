package SparkPack

object MergeList {
  def main(args: Array[String]): Unit = {
    val int_list1=List(1,2,3)
    val int_list2=List(4,5,6)
    println("============ List 1 ===================")
    int_list1.foreach(println)
    
    println("============ List 2 ===================")
    int_list2.foreach(println)
    
    println("============ Merged List================")
    int_list1.union(int_list2).foreach(println)
  }
}