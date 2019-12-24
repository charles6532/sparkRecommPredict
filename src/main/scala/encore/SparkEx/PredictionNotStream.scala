package encore.SparkEx

import scala.util.Random

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object PredictionNotStream extends Serializable {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    
    val data = spark.sqlContext.read.format("com.databricks.spark.csv").
    option("delimiter", ",").
    load("/home/encore/dt_four_dataframe_join_csv/*.csv")
	val data_rdd = data.rdd

// kafkaStream을 통과한 DataFrame
	val lines = spark.sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").load("/home/encore/streaming/predictionResults/*")

	//lines.show

	import org.apache.spark.sql.functions.udf
	val removeBracket = udf({(a:String)=>a.replace("(","").replace(")","")})

	val removedBracket_lines = lines.withColumn("_c0",removeBracket($"_c0")).withColumn("_c7",removeBracket($"_c7"))

	//refined_lines.show

	val lines_rdd = removedBracket_lines.rdd

	lines_rdd.collect

	val refinedDataRdd = data_rdd.map(x=>(x(30).toString+x(28).toString+x(33).toString+x(34).toString,x))
	val refinedLinesRdd = lines_rdd.map(x=>(x(2).toString+x(3).toString+x(4).toString+x(5).toString,x))
	val joinedrdd = refinedDataRdd.join(refinedLinesRdd)
	val refined_joined_rdd = joinedrdd.groupByKey()

	val refined_lines = refined_joined_rdd.map(str =>(str._1,str._2.toArray)).
map(str =>(str._1,str._2(0))).
map(str=>str._2._2(0)+","+str._2._2(1)+","+str._2._1(33)+","+str._2._1(10)+","+
str._2._1(14)+","+str._2._1(9)+","+str._2._1(25)+","+
str._2._1(28)+","+str._2._1(26)+","+str._2._1(15)+","+
str._2._1(24)+","+str._2._1(23)+","+str._2._1(6)+","+
str._2._1(30)
)

val lines_tmp = refined_lines.filter(! _.contains("null")).map { line =>
      val part = line.split(",")
LabeledPoint(part(1).toDouble,
	Vectors.dense(part(2).toDouble,part(3).toDouble,part(4).toDouble,part(5).toDouble,part(6).toDouble,part(7).toDouble,
	    part(8).toDouble,part(9).toDouble,part(10).toDouble,part(11).toDouble,part(12).toDouble,part(13).toDouble)
	)
}.cache()

  val refined_data = data_rdd.map(str =>
str(33)+","+str(10)+","+str(14)+","+str(9)+","+str(25)+","+str(28)+","+str(26)+","+str(15)+","+
str(8)+","+str(24)+","+str(23)+","+str(6)+","+str(30)
)

  val underTmp = refined_data.filter(! _.contains("null")).map { line =>
  val part = line.split(',')
  if (part.apply(8).toDouble<=70000) LabeledPoint(part.apply(8).toDouble ,
	Vectors.dense(part.apply(0).toDouble,part.apply(1).toDouble,part.apply(2).toDouble,part.apply(3).toDouble,part.apply(4).toDouble,
	    part.apply(5).toDouble,part.apply(6).toDouble,part.apply(7).toDouble,part.apply(9).toDouble,part.apply(10).toDouble,
	    part.apply(11).toDouble,part.apply(12).toDouble)
	)
  else(null)
}.filter(x=>x!=null).cache()

  val overTmp = refined_data.filter(! _.contains("null")).map { line =>
  val part = line.split(',')
  if (part.apply(8).toDouble>70000) LabeledPoint(part.apply(8).toDouble ,
	Vectors.dense(part.apply(0).toDouble,part.apply(1).toDouble,part.apply(2).toDouble,part.apply(3).toDouble,part.apply(4).toDouble,
	    part.apply(5).toDouble,part.apply(6).toDouble,part.apply(7).toDouble,part.apply(9).toDouble,part.apply(10).toDouble,
	    part.apply(11).toDouble,part.apply(12).toDouble)
	)
  else(null)
}.filter(x=>x!=null).cache()

  val boolFunc = ((a:Int,b:Int)=>if (a>b) true else false)

val bool =boolFunc(underTmp.count.toInt,overTmp.count.toInt)

val underSevenParsedData = if(bool==true)sc.makeRDD(underTmp.takeSample(false,overTmp.count.toInt)) else underTmp

val overSevenParsedData = if(bool==false)sc.makeRDD(overTmp.takeSample(false,underTmp.count.toInt)) else overTmp

val parsedData = underSevenParsedData.union(overSevenParsedData)

val sets = parsedData.randomSplit(Array(0.8,0.2))
val trainingData = sets(0)
val testData = sets(1)

  val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "variance"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)
	

	val labelsAndPredictions = lines_tmp.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

  
  
	val changesLabel = labelsAndPredictions.map(x=>(x._1.toInt,(x._2.toDouble)))

	//changesLabel.collect

	val userIdpropId = lines_rdd.map(str=>str(0).toString.replace("(","")+","+str(1)).map(x=>(x.split(",")(1).toInt,x.split(",")(0).toInt))

	//userIdpropId.collect

	val results = userIdpropId.join(changesLabel).map(x=>(x._2._1,x._1,x._2._2))

	// userId를 추가한 RDD를 DataFrame으로 만들 것
	val predictionDF = results.toDF("user","property","prediction")

	predictionDF.show

	predictionDF.write.format("json").save("/home/encore/predictionresult")

	
  }
}