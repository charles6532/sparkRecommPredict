package encore.SparkEx

import scala.util.Random

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


object RecommNotStream extends Serializable {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val lines = spark.sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").load("/home/encore/streaming/recommResults/*")
    val refinedLines = lines.withColumnRenamed("_c0", "user").withColumnRenamed("_c1", "property").withColumnRenamed("_c2", "count")

    import org.apache.spark.sql.functions.udf
    val udf_remove = udf({ (str: String) => str.replaceAll("\\(", "").replaceAll("\\)", "") })
    
    val linesb = refinedLines.withColumn("user", udf_remove($"user")).withColumn("count", udf_remove($"count"))

    val csvDF = linesb.withColumn("user", $"user".cast(IntegerType)).withColumn("property", $"property".cast(IntegerType)).withColumn("count", $"count".cast(IntegerType))

    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(10).
      setRegParam(0.01).
      setAlpha(1.0).
      setMaxIter(10).
      setUserCol("user").
      setItemCol("property").
      setRatingCol("count").
      setPredictionCol("prediction").
      fit(csvDF)

    model.save("/home/encore/alsmodel")

    import org.apache.spark.ml.recommendation._
    import org.apache.spark.sql.functions.explode

    val alsmodel = model
    val users = csvDF.select("user").distinct.as[Int].collect

    val builder = StringBuilder.newBuilder
    
    builder.append("user,")
    
    for  ( i<- 0 to users.length-1){
      builder.append(users(i))
      if(i != users.length-1) builder.append(",")
    }
    
    
    val arraytoStrings = builder.toString().split(",")
    val rddUsers = sc.makeRDD(arraytoStrings)
    val userSchema = StructType(
          List(
          StructField("user",IntegerType,true),
          StructField("property",IntegerType,true),
          StructField("count",IntegerType,true)
          )
        )
    
    val temDS = spark.createDataset(rddUsers)
    val dsUsers = temDS.withColumnRenamed(temDS.columns(0), "user")
    val recommendForSubsetDF = alsmodel.recommendForUserSubset(dsUsers, 10)
        
    val recommendationsDF = recommendForSubsetDF.select($"user", explode($"recommendations").alias("recommendation")).select($"user", $"recommendation.*")

    recommendationsDF.write.format("json").save("/home/encore/alsresult")
    

  }
}