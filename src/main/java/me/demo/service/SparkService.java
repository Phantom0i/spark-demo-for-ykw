package me.demo.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import scala.Tuple2;
import java.util.stream.Collectors;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import java.util.Arrays;

class TestClass implements FlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<String> call(String s) throws Exception {
        JiebaSegmenter segmenter = new JiebaSegmenter();
            List<SegToken> segs = segmenter.process(s, SegMode.SEARCH);
            // return segs.stream().map(seg -> seg.word).collect(Collectors.toList()).iterator();
            // return new ArrayList(segs.stream().map(seg -> seg.word).collect(Collectors.toList())).iterator();
            List<String> res = new ArrayList();
            for (SegToken seg : segs) {
                res.add(seg.word);
            }
            return res.iterator();
    }

}

@Service
public class SparkService implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(SparkService.class);

    @Autowired
    private transient JavaSparkContext sc;

    private JavaRDD<String> splitWords(JavaRDD<String> data) {
        return data.flatMap(s -> Arrays.asList(" ".split(s)).iterator());
        // TestClass c = new TestClass();
        // return data.flatMap(c);
        // return data.flatMap(s -> {
        //     JiebaSegmenter segmenter = new JiebaSegmenter();
        //     List<SegToken> segs = segmenter.process(s, SegMode.SEARCH);
        //     return segs.stream().map(seg -> seg.word).collect(Collectors.toList()).iterator();
        // });
    }

    private JavaPairRDD<String, Integer> wordCount(JavaRDD<String> data) {
        return data.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
    }

    private List<Tuple2<String, Integer>> topK(JavaPairRDD<String, Integer> data, int k) {
        return data.sortByKey(false).top(k);
    }

    public List<Tuple2<String, Integer>> topK(List<String> texts, int k) {
        JavaRDD<String> textRdd = sc.parallelize(texts);
        JavaRDD<String> words = splitWords(textRdd);
        JavaPairRDD<String, Integer> counts = wordCount(words);
        return topK(counts, k);
    }
}
