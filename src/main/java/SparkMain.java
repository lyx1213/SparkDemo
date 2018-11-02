import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class SparkMain {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkMain").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> parallelize = sc.parallelize(integers);
        Integer reduce = parallelize.reduce((a, b) -> a + b);
        System.out.println(reduce);

        String userHome = System.getProperty("user.home");

        JavaRDD<String> stringJavaRDD = sc.textFile(userHome + "/Documents/noteTest");
        JavaRDD<String> persist = stringJavaRDD.persist(StorageLevel.MEMORY_ONLY());
        JavaRDD<Integer> integerJavaRDD = stringJavaRDD.map(s -> s.length());

        Integer reduce1 = integerJavaRDD.reduce((a, b) -> a + b);
        System.out.println(reduce1);

    }
}
