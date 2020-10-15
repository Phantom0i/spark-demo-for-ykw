package me.demo.service;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Serializable;
import scala.Tuple2;
import java.util.stream.Collectors;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkService implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkService.class);

    @Autowired
    private transient JavaSparkContext sc;

    private JavaRDD<String> splitWords(JavaRDD<String> data) {
        return data.flatMap(s -> {
            JiebaSegmenter segmenter = new JiebaSegmenter();
            List<SegToken> segs = segmenter.process(s, SegMode.SEARCH);
            return segs.stream().map(seg -> seg.word).collect(Collectors.toList()).iterator();
        });
    }

    private JavaPairRDD<String, Integer> wordCount(JavaRDD<String> data) {
        return data.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
    }

    private List<Tuple2<String, Integer>> topK(JavaPairRDD<String, Integer> data, int k) {
        return data.sortByKey(false).top(k);
    }
}
