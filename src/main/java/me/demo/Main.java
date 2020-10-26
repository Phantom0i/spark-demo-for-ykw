package me.demo;

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


class Main {
    static String[] sentences = new String[] {
        "这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。",
        "我不喜欢日本和服。",
        "雷猴回归人间。",
        "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作", 
        "结过婚的和尚未结过婚的" 
    }; 

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("spark://study4.zhangjianglan.ws2.wh-a.brainpp.cn:7077")
            .setJars(new String[]{
                "/home/zhangjianglan/workspace/spark-test/target/spark-test-1.0.jar"})
            .set("spark.executor.extraClassPath", "/home/zhangjianglan/workspace/spark-test/target/lib/*")
            .set("spark.driver.extraClassPath", "/home/zhangjianglan/workspace/spark-test/target/lib/*")
            .setAppName("WordFrequencyStatistic"); 

        SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();
        
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> lines = jsc.parallelize(Arrays.asList(sentences));

        JavaRDD<String> words = lines.flatMap(s -> {
            JiebaSegmenter segmenter = new JiebaSegmenter();
            List<SegToken> segs = segmenter.process(s, SegMode.SEARCH);
            Stream<String> rawWords = segs.stream().map(seg -> seg.word);
            return StopWordsFilter.getInstance().filter(rawWords)
                .collect(Collectors.toList()).iterator();

        });
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<Integer, String>> output = counts.mapToPair(x -> x.swap()).sortByKey(false).take(10);
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
        jsc.close();
    } 
}
  