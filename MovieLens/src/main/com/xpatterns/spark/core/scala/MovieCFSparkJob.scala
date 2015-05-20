package com.xpatterns.spark.core.scala

import java.io.IOException

import api.{SparkJobInvalid, SparkJobValid, SparkJobValidation}
import com.typesafe.config.Config
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.util.Try

class MovieCFSparkJob extends XPatternsScalaSparkJob {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    XPatternsInstrumentation.getInstance().getLogger.info("MovieCFSparkJob.validate")
    val string: Any = config.getString("ratingsFile")
    Try(string)
      .map(x => SparkJobValid())
      .getOrElse(SparkJobInvalid("No <ratingsFile> config param defined as input file"))

    Try(config.getString("similaritiesFile"))
      .map(x => SparkJobValid())
      .getOrElse(SparkJobInvalid("No <similaritiesFile> config param defined as output file"))
  }

  override def runJob(sc: SparkContext, configuration: Config): java.io.Serializable = {

    val logger = XPatternsInstrumentation.getInstance().getLogger

    logger.debug("*Scala Spark Job Collaborative Filtering Implementation *")

    val ratingsFile: String = configuration.getString("ratingsFile")
    val similaritiesFile: String = configuration.getString("similaritiesFile")

    try {
      val hadoopConf: Configuration = new Configuration
      hadoopConf.set("fs.defaultFS", configuration.getString("hdfsUrl"))
      hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      val fs: FileSystem = FileSystem.get(hadoopConf)
      val outputHdfsPath: Path = new Path(similaritiesFile)
      if (fs.exists(outputHdfsPath)) {
        fs.delete(outputHdfsPath, true)
      }
    } catch {
      case e: IOException => {
        logger.error("Exception caught while deleting old output\n" + ExceptionUtils.getFullStackTrace(e))
      }
    }

    var initialTime = System.currentTimeMillis()
    logger.debug("Loading movies rating list from HDFS: " + ratingsFile)
    val ratings = sc.textFile(ratingsFile)
      .map(line => {
      var ratingsMetric = XPatternsInstrumentation.getInstance().registerMetric("ratings-number")
      val fields = line.split("\t")

      ratingsMetric.incrementProcessedItemsBy(1, System.currentTimeMillis() - initialTime)
      (fields(0).toLong, fields(1).toLong, fields(2).toDouble)
    }).cache

    ratings.count
    logger.debug("Input data loaded")

    logger.debug("Calculating number of raters/movie")
    val ratersGroupsBy = ratings.groupBy(tup => tup._2)

    val ratersPerMovie = ratersGroupsBy.map(grouped => {
      val movieId = grouped._1
      val noRaters = grouped._2.size

      (movieId, noRaters)
    })

    logger.debug("Adding nr. of raters in initial vector")
    val ratingsWithSize = ratersGroupsBy
      .join(ratersPerMovie)
      .flatMap(joined => {
      joined._2._1.map(f => {
        val userId = f._1
        val movieId = f._2
        val rating = f._3
        val noRaters = joined._2._2

        (userId, movieId, rating, noRaters)
      })
    })

    logger.debug("Creating 2end vector to be used for data correlation")
    val ratingsDuplicate = ratingsWithSize.keyBy(ratings => ratings._1).cache()

    val ratingPairs = ratingsDuplicate
      .join(ratingsDuplicate)
      .filter(f => f._2._2._1 < f._2._2._2)

    logger.debug("Preparing vector for data correlation")
    initialTime = System.currentTimeMillis()
    val intermediateCalculations =
      ratingPairs
        .map(data => {
        val key = (data._2._1._2, data._2._2._2)
        val stats =
          (data._2._1._3 * data._2._2._3,
            data._2._1._3,
            data._2._2._3,
            math.pow(data._2._1._3, 2),
            math.pow(data._2._2._3, 2),
            data._2._1._4,
            data._2._2._4)

        (key, stats)
      })
        .groupByKey()
        .map(data => {
        val key = data._1
        val vals = data._2
        val size = vals.size
        val dotProduct = vals.map(f => f._1).sum
        val ratingSum = vals.map(f => f._2).sum
        val rating2Sum = vals.map(f => f._3).sum
        val ratingSq = vals.map(f => f._4).sum
        val rating2Sq = vals.map(f => f._5).sum
        val numRaters = vals.map(f => f._6).max
        val numRaters2 = vals.map(f => f._7).max

        (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
      })

    logger.debug("Calculating correlation factor for each pair of movies")
    val similarities =
      intermediateCalculations
        .map(fields => {
        val smiliartiesMetric = XPatternsInstrumentation.getInstance().registerMetric("similar-movies-pairs")

        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val corr = (size * dotProduct - ratingSum * rating2Sum) / (math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum))

        smiliartiesMetric.incrementProcessedItemsBy(1, System.currentTimeMillis() - initialTime)
        (key, corr)
      })

    logger.debug("Writing correlation movie list to output")
    similarities.filter(tuple => !(tuple._2 equals Double.NaN))
      .map(tuple => "%s,%s,%s".format(tuple._1._1, tuple._1._2, tuple._2))
      .saveAsTextFile(similaritiesFile);

    logger.debug("Output is:" + similaritiesFile)

    return null
  }

}
