package com.xpatterns.ami.sample

import api._
import com.typesafe.config.Config
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger
import com.xpatterns.spark.core.java.instrumentation.metrics.XPatternsMetrics
import com.xpatterns.spark.core.scala.XPatternsScalaSparkJob
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Created by radum on 26.04.2015.
 */
class SampleScalaSparkJob extends XPatternsScalaSparkJob {


  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    XPatternsInstrumentation.getInstance().getLogger.info("***** SampleScalaSparkJob - validate !!!")
    Try(config.getString("input"))
      .map(x => SparkJobValid())
      .getOrElse(SparkJobInvalid("No <input> config param"))
  }


  override def runJob(sc: SparkContext, config: Config): java.io.Serializable = {

    val log: XPatternsLogger = XPatternsInstrumentation.getInstance().getLogger

    log.info("-------------------- SampleScalaSparkJob run -------------------------");
    log.info("*SCALA* SampleScalaSparkJob - runjob !!! parameter INPUT: " + config.getString("input"))


    val totalItems: XPatternsMetrics = XPatternsInstrumentation.getInstance.registerMetric("items")

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6).map(_ * 10))
    log.info("*SCALA* Result is " + rdd.collect.toSeq.sum)

    rdd.collect.foreach(item => {

      log.info("*SCALA* [original RDD]: " + item)
      val initial: Long = System.currentTimeMillis
      totalItems.incrementProcessedItemsBy(1, System.currentTimeMillis - initial)
      Thread.sleep(2000)
    }
    )
    val rdd2 = rdd.map(_ * 10)

    log.info("*SCALA* [altered RDDs] -> call .map(_ * 10)")
    rdd2.collect.foreach(item2 => log.info("*SCALA* [altered RDD]: " + item2))

    log.info("-------------------- SampleScalaSparkJob done ... final results :..-------------------------");



    return "done :)"
  }
}
   