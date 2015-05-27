package com.xpatterns.enron.app

import api.{ SparkJobValid, SparkJobValidation }

import com.typesafe.config.Config
import com.xpatterns.spark.core.java.XPatternsSparkJobClient
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation
import com.xpatterns.spark.core.scala.XPatternsScalaSparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import java.io.{ BufferedOutputStream, ByteArrayOutputStream, File, FileNotFoundException, PrintWriter }
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.zip.{ ZipEntry, ZipInputStream }
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataInputStream, FSDataOutputStream, FileSystem, Path }
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.{ BytesWritable, NullWritable, SequenceFile }
import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ Accumulable, SparkContext }
import org.objenesis.strategy.StdInstantiatorStrategy
import scala.collection.Map
import scala.collection.mutable.{ HashSet, ListBuffer, PriorityQueue, Queue }
import scala.util.control.Breaks._
import org.apache.spark.sql.parquet.SparkParquetUtility._
import org.apache.spark.io.CompressionCodec
import parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import java.io.IOException
import java.io.FileInputStream
import com.xpatterns.enron.data.Message

class EmailIngestion extends XPatternsScalaSparkJob with Serializable {

  override def runJob(sc: SparkContext, jobConfig: Config): java.io.Serializable = {

    val logger = XPatternsInstrumentation.getInstance().getLogger
    val config: IngestionConfiguration = IngestionConfiguration(jobConfig)

    val inputFsAdress = config.inputFS
    val inputFS = FileSystem.newInstance(new URI((inputFsAdress)), new Configuration())
    val outputFS = FileSystem.newInstance(new URI((config.outputFs)), new Configuration())

    if (outputFS.exists(new Path(config.outputPath)) && config.overwrite) {
      logger.warn("Output folder already exists, deleting folder : " + config.outputPath)
      outputFS.delete(new Path(config.outputPath), true)
    } else if (outputFS.exists(new Path(config.outputPath))) {
      logger.error("Output folder already exists. Overwrite is disabled, shutting down the app!")
      System.exit(-1)
    }

    val errorsAcc = sc.accumulableCollection(HashSet[String]())
    val fileAcc = sc.accumulator(0L)

    val mailsList = getFilesFromHdfs(config.inputPath, inputFS)
    val pathRdd = sc.parallelize(mailsList, config.parallelizeFactor)

    val mailsRDD = pathRdd.mapPartitions(iterator => {

      val processedMetric = XPatternsInstrumentation.getInstance().registerMetric("processed.message")
      val attachmentMetric = XPatternsInstrumentation.getInstance().registerMetric("processed.attachment")

      val fs = FileSystem.newInstance(new URI(inputFsAdress), new Configuration())

      val list = new ListBuffer[Message]()
      var stream: FSDataInputStream = null

      while (iterator.hasNext) {
        val path = iterator.next

        val initialTime = System.currentTimeMillis()
        try {
          stream = fs.open(new Path(path))
          val msg = Message.readEml(stream)

          list += msg
          fileAcc.+=(1L)
          if (msg.attachments.size > 0)
            attachmentMetric.incrementProcessedItemsBy(msg.attachments.size, System.currentTimeMillis() - initialTime)

          processedMetric.incrementProcessedItemsBy(1, System.currentTimeMillis() - initialTime)
        } catch {
          case e: Exception =>
            {
              val errorMessage = s"Error while reading file  ${path}"
              errorsAcc.add(errorMessage)
              processedMetric.incrementFailedItemsBy(1)
            }
        } finally {
          stream.close()
        }
      }
      list.iterator
    })

    mailsRDD.saveAsXPatternsParquet(config.outputFs, config.outputPath, parquetBlockSize = 25 * 1024 * 1024, codec = config.compression)

    if (errorsAcc.value.size > 0) {

      val errorFile = config.errorsFilePath
      val dateFormat = new SimpleDateFormat("yyyyMMdd-hhmm")
      val sepIndex = errorFile.lastIndexOf(".")
      val finalErrorFilePath: String =
        if (sepIndex > 0)
          errorFile.substring(0, sepIndex - 1) + dateFormat.format(new Date()) + errorFile.substring(sepIndex, errorFile.size)
        else
          errorFile + dateFormat.format(new Date())

      var outputStream: FSDataOutputStream = null
      try {
        outputStream = outputFS.create(new Path(finalErrorFilePath), true)
        errorsAcc.value.foreach(file => outputStream.write((file + "\n").getBytes()))
        outputStream.flush()
      } catch {
        case x: Exception => {
          logger.error("Error while writing the errors files : " + x.getMessage)
          x.printStackTrace()
        }
      } finally {
        outputStream.close()
        outputFS.close()
      }

      logger.error("There were errors processing " + errorsAcc.value.size + " files.")
      logger.error("File path with full stack trace: " + finalErrorFilePath)
    }

    logger.info("Total processed mails : " + fileAcc.value)
    true
  }

  def getFilesFromHdfs(inputFolderToScanForfiles: String, fs: FileSystem) = {
    val inputFileNames = ListBuffer[String]()
    val inputPath = new Path(inputFolderToScanForfiles)

    var foldersQueue = ListBuffer[String]()
    foldersQueue.+=(inputFolderToScanForfiles)

    while (foldersQueue.size > 0) {
      val workingInputPath = foldersQueue.head
      foldersQueue = foldersQueue.tail
      val status = fs.listStatus(new Path(workingInputPath))

      status.foreach(fileStatus => {

        if (fileStatus.isFile() == true) {
          inputFileNames.+=(fileStatus.getPath().toUri().getPath());
        }

        if (fileStatus.isDirectory() == true) {
          foldersQueue.+=(fileStatus.getPath().toUri().getPath());
        }
      })
    }

    inputFileNames
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    return SparkJobValid()
  }
}

