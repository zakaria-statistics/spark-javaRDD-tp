package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application {
    public static void main(String[] args) {


//Configuration of Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("tp-rdd")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("OFF");
//Parallalising a list
        List<String> names = Arrays.asList("ADwight", "BMichael", "CJIm", "AOscar", "BPam", "CKevin", "AErin");
        JavaRDD<String> rdd1 = sparkContext.parallelize(names);
//flatMap transformation
        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

// Transformation filter
        JavaRDD<String> rdd3 = rdd2.filter(s -> s.startsWith("A"));
        JavaRDD<String> rdd4 = rdd2.filter(s -> s.startsWith("B"));
        JavaRDD<String> rdd5 = rdd2.filter(s -> s.startsWith("C"));

// Transformation union
        JavaRDD<String> rdd6 = rdd3.union(rdd4);
        JavaRDD<String> rdd7 = rdd5.map(String::toUpperCase);

        JavaPairRDD<String, Integer> rdd8 = rdd6.union(rdd7)
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);

        JavaPairRDD<Integer, String> rdd9 = rdd8.mapToPair(Tuple2::swap);
        JavaRDD<String> rdd10 = rdd9.map(Tuple2::_2);

// Affichage du résultat final
        System.out.println("RDD10:");
        for (String s : rdd10.collect()) {
            System.out.println(s);
        }

// Arrêt de Spark
        sparkContext.stop();

    }
}