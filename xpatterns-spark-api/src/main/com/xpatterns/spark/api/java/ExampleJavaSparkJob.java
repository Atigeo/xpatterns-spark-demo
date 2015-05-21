package com.xpatterns.spark.api.java;

import com.typesafe.config.Config;
import com.xpatterns.spark.core.java.XPatternsJavaSparkJob;
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation;
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;
import com.xpatterns.spark.core.java.validation.XPatternsJobValid;
import com.xpatterns.spark.core.java.validation.XPatternsJobValidation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by radum on 26.04.2014.
 */
public class ExampleJavaSparkJob implements Serializable, XPatternsJavaSparkJob {

    public XPatternsJobValidation validate(JavaSparkContext javaSparkContext,
                                           Config configuration) {

        return new XPatternsJobValid("OK");
    }

    public Serializable run(JavaSparkContext sc, Config configuration) {

        XPatternsLogger log = XPatternsInstrumentation.getInstance().getLogger();
        log.info("-------------------- SampleJavaSparkJob run -------------------------");

        String hdfsPath = configuration.getString("input");
        log.info("*JAVA* HDFS Source: " + hdfsPath);


        JavaRDD<String> lines = sc.textFile(hdfsPath);
        for (String line : lines.collect()) {
            log.info("*JAVA* Line: " + line);
        }

        log.info("*JAVA* running JavaRDD<String> words = lines.flatMap(...");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {

                return Arrays.asList(s.split(" "));
            }
        });

        long count = words.count();
        log.info("*JAVA* Results WordCount:" + count);
        log.info("-------------------- SampleJavaSparkJob done -------------------------");
        return count;
    }


}
