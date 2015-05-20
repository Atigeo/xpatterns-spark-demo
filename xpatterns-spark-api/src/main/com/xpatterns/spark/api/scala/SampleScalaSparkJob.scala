package com.xpatterns.spark.core.scala

import api.{SparkJobInvalid, SparkJobValid, SparkJobValidation}
import com.typesafe.config.Config
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Created by radum on 26.04.2014.
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

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6).map(_ * 10))
    log.info("*SCALA* Result is " + rdd.collect.toSeq.sum)

    rdd.collect.foreach(item => log.info("*SCALA* [original RDD]: " + item))
    val rdd2 = rdd.map(_ * 10)

    log.info("*SCALA* [altered RDDs] -> call .map(_ * 10)")
    rdd2.collect.foreach(item2 => log.info("*SCALA* [altered RDD]: " + item2))

    log.info("-------------------- SampleScalaSparkJob done ... final results :..-------------------------");


    /*
     log.info("-------------------- SampleScalaSparkJob run -------------------------");
     //generic read params sequence
     log.info("*SCALA* SampleScalaSparkJob - runjob !!! parameter INPUT: " + config.getString("input"))

     log.info("*SCALA* [create RDDs]")
     val rdd = namedRdds.getOrElseCreate("rdd_test_1", {
       // anonymous generator function
       sc.parallelize(Seq(1, 2, 3, 4, 5, 6).map(_ * 10))
     })
     log.info("*SCALA* Result is " + rdd.collect.toSeq.sum)

     namedRdds.getOrElseCreate("rdd_test_2", {
       // anonymous generator function
       sc.parallelize(Seq("foot", "football", "foobar"))
     })

      // RDD should already be in cache the second time
     log.info( "*SCALA* List of namedRdds..." )
      namedRdds.getNames.foreach( rdd => log.info("Rdd: " + rdd))

     // retrieve RDDs
     log.info("*SCALA* [retrieve RDDs]")
     val rddTest1:Option[RDD[Int]] = namedRdds.get[Int]("rdd_test_1")

     log.info("*SCALA* [original RDDs]")
     rddTest1.get.collect.foreach(item => log.info("*SCALA* [original RDD]: "+ item))

     //alter RDDs
     log.info("*SCALA* [altered RDDs] ")
     val newRDDTest1 = rddTest1.get.map(_ * 10)

     log.info("*SCALA* [altered RDDs] -> call .map(_ * 10)")
     newRDDTest1.collect.foreach(item => log.info("*SCALA* [altered RDD]: "+ item))

     //update RDDs
     log.info("*SCALA* [update RDDs]")
     namedRdds.update("rdd_test_1", {newRDDTest1})

    val collectedResults: Array[Int] = newRDDTest1.collect

    log.info("-------------------- SampleScalaSparkJob done ... final results :..-------------------------");

*/
    return null
  }
}
   