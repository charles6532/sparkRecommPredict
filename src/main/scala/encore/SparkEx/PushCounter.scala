package encore.SparkEx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType,LongType,BooleanType,DateType}



object PushCounter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Github Push Counter")
      .master("spark://master:7077")
      .getOrCreate()
    val sc = spark.sparkContext
    val schema = StructType(
      List(
        StructField("id", LongType, true),
        StructField("type", StringType, true),
        StructField("actor", StringType, true),
        StructField("repo", StringType, true),
        StructField("payload", StringType, true),
        StructField("public", BooleanType, true),
        StructField("created_at", DateType, true),
        
      )
    )
    // val homedir = System.getenv("HOME")
    val logs = spark.readStream.schema(schema).json("/home/encore/data/2019-10-01-3.json") // DataFrame
    val pushes = logs.filter("type='PushEvent'")
    pushes.printSchema()
    
    
    print("All Event : " + logs.count())
    print("Push Event : " + pushes.count())
    pushes.show(1)

    val groups = pushes.groupBy("actor.login").count() // push counter
    var ordered = groups.orderBy(groups("count").desc) // user who pushed most

    println("=======================================")
    groups.show(3)
    println()
    ordered.show(3)
    println("=======================================")
    val query1 = logs.writeStream.format("console").start()
    logs.printSchema()
    query1.awaitTermination()
    
//    import scala.io.Source.fromFile
//    val employees = Set() ++ (
//      for {
//        line <- fromFile("/home/encore/ghEmployees.txt").getLines()
//      } yield line.trim)
//
//    val broadcastEmployees = sc.broadcast(employees)
//
//    import spark.implicits._
//    val isExist = user => broadcastEmployees.value.contains(user)
//    val isEmployees = spark.udf.register("SetContainsUdf", isExist)
//    val filtered = ordered.filter(isEmployees($"login"))
//    filtered.show()
//    filtered.write.format("json").save("/home/encore/pushcount/")
  }
}
