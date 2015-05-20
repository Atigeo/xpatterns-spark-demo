package com.xpatterns.spark.core.java;

import com.typesafe.config.Config;
import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation;
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;
import com.xpatterns.spark.core.java.validation.XPatternsJobInvalid;
import com.xpatterns.spark.core.java.validation.XPatternsJobValid;
import com.xpatterns.spark.core.java.validation.XPatternsJobValidation;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import static java.util.Arrays.asList;

/**
 * Created by radum on 26.04.2014.
 */
@SuppressWarnings("serial")
public class DemoJavaSparkJob implements Serializable, XPatternsJavaSparkJob {

    static final String COMMA_DELIMITER = ",";

    @Override
    public XPatternsJobValidation validate(JavaSparkContext javaSparkContext, Config configuration) {
        XPatternsLogger log = XPatternsInstrumentation.getInstance().getLogger();
        log.info("-------------------- DemoJavaSparkJob validate -------------------------");

        String input = configuration.getString("input");
        String output = configuration.getString("output");

        if (input == null) {
            return new XPatternsJobInvalid("No <input> parameter was given!!!!");
        }

        if (output == null) {
            return new XPatternsJobInvalid("No <output> parameter was given!!!!");
        }

        return new XPatternsJobValid("OK! input:<" + input + "> output:<" + output + ">");
    }

    public Serializable run(JavaSparkContext javaSparkContext, Config configuration) {

        XPatternsLogger log = XPatternsInstrumentation.getInstance().getLogger();
        log.info("-------------------- DemoJavaSparkJob run -------------------------");


        String input = configuration.getString("input");
        String output = configuration.getString("output");
        log.info("INPUT:" + input);
        log.info("OUTPUT:" + output);

        // Spark validates output location and throws exception if it already exists.
        // Delete it to prevent this.
        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.defaultFS", configuration.getString("hdfsUrl"));
            hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(hadoopConf);
            Path outputHdfsPath = new Path(output);
            if (fs.exists(outputHdfsPath)) {
                fs.delete(outputHdfsPath, true);
            }
        } catch (IOException e) {
            log.error("Exception caught while deleting old output\n" + ExceptionUtils.getFullStackTrace(e));
        }

        JavaRDD<String> lines = javaSparkContext.textFile(input);

        JavaRDD<String> correction = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String line) {

                String[] columns = line.split(COMMA_DELIMITER);
                columns[1] = columns[1].replaceAll("_", " ");

                return new ArrayList<String>(asList(rebuildLine(columns)));
            }

            private String rebuildLine(String[] columns) {
                StringBuilder sb = new StringBuilder();
                for (String n : columns) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(n);
                }
                return sb.toString();
            }
        });

        correction.saveAsTextFile(output);
        log.info("-------------------- DemoJavaSparkJob done -------------------------");

        return "Job done!";

    }

}
