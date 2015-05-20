package com.xpatterns.spark.core.java;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation;
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;
import com.xpatterns.spark.core.java.validation.XPatternsJobValid;
import com.xpatterns.spark.core.java.validation.XPatternsJobValidation;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Created by radum on 26.04.2014.
 */
@SuppressWarnings("serial")
public class FileConfiguredJavaSparkJob implements Serializable, XPatternsJavaSparkJob {

    public XPatternsJobValidation validate(JavaSparkContext javaSparkContext,
                                           Config configuration) {

        return new XPatternsJobValid("OK");
    }

    public Serializable run(JavaSparkContext javaSparkContext, Config configuration) {


        XPatternsLogger log = XPatternsInstrumentation.getInstance().getLogger();
        log.info("-------------------- SampleJavaSparkJob run -------------------------");

        log.info("*JAVA* SampleJavaSparkJob - configuration values: ***************");
        for (Entry<String, ConfigValue> entry : configuration.entrySet()) {
            String key = entry.getKey();
            ConfigValue value = entry.getValue();
            if (key.startsWith("spark")) {
                log.info("Key:" + key + " value:" + value.toString());
            }
        }

        String configFile = configuration.getString("configFile");

        byte[] resourceContent;
        try {
            resourceContent = XPatternsSparkJobResourceManager.getResourceByFilePath(configFile, configuration, log);
            log.info("*JAVA* SampleJavaSparkJob -> get Resource by name " + configFile + ":\n" + new String(resourceContent));

        } catch (Exception e) {
            log.error("*JAVA* Exception caught while reading resource file " + configFile);
            throw new RuntimeException(e);
        }

        Properties sparkJobProperties = new Properties();
        try {
            sparkJobProperties.load(new ByteArrayInputStream(resourceContent));
            String hdfsPath = sparkJobProperties.getProperty("hdfsPath");
            String hdfsURL = sparkJobProperties.getProperty("hdfsUrl");
            String hdfsPORT = sparkJobProperties.getProperty("hdfsPort");
            String hdfsOutput = sparkJobProperties.getProperty("hdfsOutput");

            // Spark validates output location and throws exception if it already exists.
            // Delete it to prevent this.
            try {
                Configuration hadoopConf = new Configuration();
                hadoopConf.set("fs.defaultFS", hdfsURL);
                hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem fs = FileSystem.get(hadoopConf);
                Path outputHdfsPath = new Path(hdfsOutput);
                if (fs.exists(outputHdfsPath)) {
                    fs.delete(outputHdfsPath, true);
                }
            } catch (IOException e) {
                log.error("Exception caught while deleting old output\n" + ExceptionUtils.getFullStackTrace(e));
            }

            String hdfsResource = "hdfs://" + hdfsURL + ":" + hdfsPORT + hdfsPath;
            log.info("*JAVA* HDFS Source: " + hdfsResource);
            JavaRDD<String> lines = javaSparkContext.textFile(hdfsResource);
            for (String line : lines.collect()) {
                log.info("*JAVA* Line: " + line);
            }

            log.info("*JAVA* running JavaRDD<String> words = lines.flatMap(...");
            @SuppressWarnings("serial")
            JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                public Iterable<String> call(String s) {

                    //log.info("*JAVA* running FlatMapFunction***:" + s.split(" "));
                    return Arrays.asList(s.split(" "));
                }
            });

            long count = words.count();
            log.info("*JAVA* Results WordCount:" + count);
            words.saveAsTextFile(hdfsOutput);
            log.info("-------------------- SampleJavaSparkJob done -------------------------");
            return count;
        } catch (IOException e) {
            log.error("*JAVA* Exception caught while creating properties from resource content");
            throw new RuntimeException(e);
        }

    }


}
