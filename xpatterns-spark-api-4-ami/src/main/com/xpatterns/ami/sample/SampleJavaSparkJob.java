package com.xpatterns.ami.sample;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.xpatterns.spark.core.java.XPatternsJavaSparkJob;
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation;
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;
import com.xpatterns.spark.core.java.instrumentation.metrics.XPatternsMetrics;
import com.xpatterns.spark.core.java.validation.XPatternsJobValid;
import com.xpatterns.spark.core.java.validation.XPatternsJobValidation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

/**
 * Created by radum on 26.04.2015.
 */
@SuppressWarnings("serial")
public class SampleJavaSparkJob implements Serializable, XPatternsJavaSparkJob {

    public XPatternsJobValidation validate(JavaSparkContext javaSparkContext,
                                           Config configuration) {

        return new XPatternsJobValid("OK");
    }

    public Serializable run(JavaSparkContext javaSparkContext, Config configuration) {
        XPatternsInstrumentation xpatternsInstrumentation = XPatternsInstrumentation.getInstance();
        XPatternsLogger log = xpatternsInstrumentation.getLogger();
        log.info("-------------------- SampleJavaSparkJob run -------------------------");

        log.info("*JAVA* SampleJavaSparkJob - configuration values: ***************");
        for (Entry<String, ConfigValue> entry : configuration.entrySet()) {
            String key = entry.getKey();
            ConfigValue value = entry.getValue();
            if (key.startsWith("spark")) {
                log.info("Key:" + key + " value:" + value.toString());
            }
        }

        ArrayList<String> list = new ArrayList<String>(Arrays.asList("line one", "line two", "line three"));

        JavaRDD<String> lines = javaSparkContext.parallelize(list, 1);

        XPatternsMetrics linesMetric = xpatternsInstrumentation.registerMetric("lines");
        long initial = System.currentTimeMillis();
        for (String line : lines.collect()) {
            log.info("*JAVA* Line: " + line);
            linesMetric.incrementProcessedItemsBy(1, System.currentTimeMillis() - initial);
        }

        log.info("*JAVA* running JavaRDD<String> words = lines.flatMap(...");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) throws InterruptedException {
                XPatternsMetrics wordMetrics = XPatternsInstrumentation.getInstance().registerMetric("line.process");
                XPatternsInstrumentation.getInstance().getLogger().info("* JAVA* Flat Map Function String: " + s);
                Long initial = System.currentTimeMillis();
                wordMetrics.incrementProcessedItemsBy(1, System.currentTimeMillis() - initial);
                Thread.sleep(5000);
                return Arrays.asList(s.split(" "));
            }
        });

        long count = words.count();
        log.info("*JAVA* Results WordCount:" + count);
        words.saveAsTextFile("/tmp/" + Math.random() + "_out.txt");
        log.info("-------------------- SampleJavaSparkJob done -------------------------");
        return count;
    }


}
