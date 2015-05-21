package com.xpatterns.enron.app

import com.typesafe.config.Config
import org.apache.spark.io.CompressionCodec
import parquet.hadoop.metadata.CompressionCodecName

class IngestionConfiguration extends Serializable {
  var inputFS = ""
  var outputFs = ""
  var inputPath = ""
  var outputPath = ""
  var overwrite = false
  var compression = CompressionCodecName.UNCOMPRESSED
  var errorsFilePath = ""
  var parallelizeFactor = 12
}

object IngestionConfiguration {

  def apply(config: Config): IngestionConfiguration = {
    val configuration = new IngestionConfiguration()

    configuration.inputFS = getValueFromConfig(config, "app.input.fs", "hdfs://localhost:8020")
    configuration.outputFs = getValueFromConfig(config, "app.output.fs", "")
    configuration.parallelizeFactor = getValueFromConfig(config, "app.parallelize.factor", 12)

    configuration.inputPath = getValueFromConfig(config, "app.input.path", "")
    configuration.outputPath = getValueFromConfig(config, "app.output.path", "output")

    configuration.overwrite = getValueFromConfig(config, "app.overwrite", true)
    configuration.errorsFilePath = getValueFromConfig(config, "app.unprocessed.path", "ingestion_errors")

    configuration.compression = getValueFromConfig(config, "app.compression.codec", "UNCOMPRESSED") match {
      case "GZIP" => CompressionCodecName.GZIP
      case "SNAPPY" => CompressionCodecName.SNAPPY
      case _ => CompressionCodecName.UNCOMPRESSED
    }

    configuration
  }

  def getValueFromConfig[T](config: Config, configPath: String, defaultValue: T): T = {
    return if (config.hasPath(configPath)) config.getAnyRef(configPath).asInstanceOf[T] else defaultValue
  }
}
