import java.util.Arrays;
import java.lang.Iterable;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.*;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class WordCount {

    /** Used to run the driver in-process, mostly for testing. */
    static final String CONF_KEY_IN_PROCESS = "spark.client.do_not_use.run_driver_in_process";

    private static final HiveConf HIVECONF = new HiveConf();

    private static Map<String, String> createConf(boolean local) {
        Map<String, String> conf = new HashMap<String, String>();
        if (local) {
            conf.put(CONF_KEY_IN_PROCESS, "true");
            conf.put("spark.master", "local");
            conf.put("spark.app.name", "SparkClientSuite Local App");
        } else {
            String classpath = System.getProperty("java.class.path");
            conf.put("spark.master", "local");
            conf.put("spark.app.name", "SparkClientSuite Remote App");
            conf.put("spark.driver.extraClassPath", classpath);
            conf.put("spark.executor.extraClassPath", classpath);
        }

        if (!Strings.isNullOrEmpty(System.getProperty("spark.home"))) {
            conf.put("spark.home", System.getProperty("spark.home"));
        }

        return conf;
    }

    
    public static void main(String[] args) throws Exception {

        Map<String, String> conf = createConf(true);

        SparkClientFactory.initialize(conf);
        SparkClient client = SparkClientFactory.createClient(conf, HIVECONF);

        JobHandle<Long> handle = client.submit(new SparkJob());
        System.out.println(handle.getState());
    }

    private static class SparkJob implements Job<Long> {

        public Long call(JobContext jc) {
            String inputFile = "text.txt";
            String outputFile = "text1";
            // Load our input data.
            JavaRDD<String> input = jc.sc().textFile(inputFile);
            // Split up into words.
            JavaRDD<String> words = input.flatMap(
                    new FlatMapFunction<String, String>() {
                        public Iterable<String> call(String x) {
                            return Arrays.asList(x.split(" "));
                        }
                    });
            // Transform into word and count.
            JavaPairRDD<String, Integer> counts = words.mapToPair(
                    new PairFunction<String, String, Integer>(){
                        public Tuple2<String, Integer> call(String x){
                            return new Tuple2(x, 1);
                        }}).reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer x, Integer y) {
                    return x + y;
                }
            });

            // Save the word count back out to a text file, causing evaluation.
            counts.saveAsTextFile(outputFile);
            return 1L;
        }
    }
}