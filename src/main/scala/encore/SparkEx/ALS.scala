// 음악 추천과 Audioscrobbler DataSet

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


object RunRecommender {
	def main(args: Array[String]): Unit = {
	val spark = SparkSession.builder().getOrCreate()
	spark.sparkContext.setCheckpointDir("hdfs:///tmp/")

		val base = "/input/spark/"
		val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
	val rawArtistData = spark.read.textFile(base + "artist_data.txt")
	val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

	val runRecommender = new RunRecommender(spark)
	runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
	runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
	runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
	runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)
	}
}

class RunRecommender(private val spark: SparkSession) {
	import spark.implicits._
	def preparation(
      rawUserArtistData: Dataset[String],
      rawArtistData: Dataset[String],
      rawArtistAlias: Dataset[String]): Unit = {
		rawUserArtistData.take(5).foreach(println)
			val userArtistDF = rawUserArtistData.map { line =>
			val Array(user, artist, _*) = line.split(' ')
			(user.toInt, artist.toInt)
			}.toDF("user", "artist")
	userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()
	val artistByID = buildArtistByID(rawArtistData)
	val artistAlias = buildArtistAlias(rawArtistAlias)

	val (badID, goodID) = artistAlias.head
	artistByID.filter($"id" isin (badID, goodID)).show()
	}

	def model(
	rawUserArtistData: Dataset[String],
	rawArtistData: Dataset[String],
	rawArtistAlias: Dataset[String]): Unit = {
		val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
		val trainData = buildCounts(rawUserArtistData, bArtistAlias).cache()
			val model = new ALS().
			setSeed(Random.nextLong()).
			setImplicitPrefs(true).
			setRank(10).
			setRegParam(0.01).
			setAlpha(1.0).
			setMaxIter(5).
			setUserCol("user").
			setItemCol("artist").
			setRatingCol("count").
			setPredictionCol("prediction").
			fit(trainData)
	trainData.unpersist()
	model.userFactors.select("features").show(truncate = false)

	val userID = 2093760
	val existingArtistIDs = trainData.filter($"user" === userID).select("artist").as[Int].collect()
	val artistByID = buildArtistByID(rawArtistData)
	artistByID.filter($"id" isin (existingArtistIDs:_*)).show()
	val topRecommendations = makeRecommendations(model, userID, 5)
	topRecommendations.show()
	val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
	artistByID.filter($"id" isin (recommendedArtistIDs:_*)).show()
	model.userFactors.unpersist()
	model.itemFactors.unpersist()
	}

	def evaluate(
	rawUserArtistData: Dataset[String],
	rawArtistAlias: Dataset[String]): Unit = {
		val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
		val allData = buildCounts(rawUserArtistData, bArtistAlias)
		val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
		trainData.cache()
		cvData.cache()
		val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
		val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

		val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
		println(mostListenedAUC)
		val evaluations =
			for (rank     <- Seq(5,  30);
			regParam <- Seq(1.0, 0.0001);
			alpha    <- Seq(1.0, 40.0))
			yield {
			val model = new ALS().
			  setSeed(Random.nextLong()).
			  setImplicitPrefs(true).
			  setRank(rank).setRegParam(regParam).
			  setAlpha(alpha).setMaxIter(20).
			  setUserCol("user").setItemCol("artist").
			  setRatingCol("count").setPredictionCol("prediction").
			  fit(trainData)

				val auc = areaUnderCurve(cvData, bAllArtistIDs, model.transform)
			model.userFactors.unpersist()
				model.itemFactors.unpersist()
		(auc, (rank, regParam, alpha))
			}
		evaluations.sorted.reverse.foreach(println)
	trainData.unpersist()
		cvData.unpersist()
	}

	def recommend(
      rawUserArtistData: Dataset[String],
      rawArtistData: Dataset[String],
      rawArtistAlias: Dataset[String]): Unit = {
		val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
		val allData = buildCounts(rawUserArtistData, bArtistAlias).cache()
		val model = new ALS().
		      setSeed(Random.nextLong()).
		      setImplicitPrefs(true).
		      setRank(10).setRegParam(1.0).setAlpha(40.0).setMaxIter(20).
		      setUserCol("user").setItemCol("artist").
		      setRatingCol("count").setPredictionCol("prediction").
		      fit(allData)
	allData.unpersist()

	val userID = 2093760
	val topRecommendations = makeRecommendations(model, userID, 5)
	val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
	val artistByID = buildArtistByID(rawArtistData)
	artistByID.join(spark.createDataset(recommendedArtistIDs).toDF("id"), "id").select("name").show()

	model.userFactors.unpersist()
	model.itemFactors.unpersist()
	}
	
	def buildArtistByID(rawArtistData: Dataset[String]): DataFrame = {
	rawArtistData.flatMap { line =>
		val (id, name) = line.span(_ != '\t')
			if (name.isEmpty) {
				None
		} else {
			try {
			Some((id.toInt, name.trim))
			} catch {
			case _: NumberFormatException => None
			}
			}
		}.toDF("id", "name")
	}

	def buildArtistAlias(rawArtistAlias: Dataset[String]): Map[Int,Int] = {
	rawArtistAlias.flatMap { line =>
		val Array(artist, alias) = line.split('\t')
		if (artist.isEmpty) {
			None
		} else {
			Some((artist.toInt, alias.toInt))
		}
	}.collect().toMap
	}

	def buildCounts(
	rawUserArtistData: Dataset[String],
	bArtistAlias: Broadcast[Map[Int,Int]]): DataFrame = {
	rawUserArtistData.map { line =>
		val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
		val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
		(userID, finalArtistID, count)
	}.toDF("user", "artist", "count")
	}

	def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
	val toRecommend = model.itemFactors.
		select($"id".as("artist")).
		withColumn("user", lit(userID))
	model.transform(toRecommend).
		select("artist", "prediction").
		orderBy($"prediction".desc).
		limit(howMany)
	}

	def areaUnderCurve(
      positiveData: DataFrame,
      bAllArtistIDs: Broadcast[Array[Int]],
      predictFunction: (DataFrame => DataFrame)): Double = {

	val positivePredictions = predictFunction(positiveData.select("user", "artist")).
		withColumnRenamed("prediction", "positivePrediction")

		val negativeData = positiveData.select("user", "artist").as[(Int,Int)].
		groupByKey { case (user, _) => user }.
		flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
			val random = new Random()
			val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
			val negative = new ArrayBuffer[Int]()
			val allArtistIDs = bAllArtistIDs.value
			var i = 0
			while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
			val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
			if (!posItemIDSet.contains(artistID)) {
				negative += artistID
			}
			i += 1
			}
			negative.map(artistID => (userID, artistID))
		}.toDF("user", "artist")

		val negativePredictions = predictFunction(negativeData).
		withColumnRenamed("prediction", "negativePrediction")
	val joinedPredictions = positivePredictions.join(negativePredictions, "user").
		select("user", "positivePrediction", "negativePrediction").cache()

	val allCounts = joinedPredictions.groupBy("user").agg(count(lit("1")).as("total")).select("user", "total")
	val correctCounts = joinedPredictions.filter($"positivePrediction" > $"negativePrediction").
		groupBy("user").agg(count("user").as("correct")).select("user", "correct")

	val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
		select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
		agg(mean("auc")).
		as[Double].first()

	joinedPredictions.unpersist()
	meanAUC
	}

	def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
	val listenCounts = train.groupBy("artist").
		agg(sum("count").as("prediction")).
		select("artist", "prediction")
	allData.
		join(listenCounts, Seq("artist"), "left_outer").
		select("user", "artist", "prediction")
	}
}
