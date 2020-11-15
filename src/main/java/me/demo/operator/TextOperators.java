
package me.demo.operator;

import scala.Serializable;
import scala.Tuple2;
import java.util.stream.Collectors;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Comparator;
import java.util.List;

public class TextOperators implements Serializable {
    private static final long serialVersionUID = 1L;

    public JavaRDD<String> splitWords(JavaRDD<String> data) {
        return data.flatMap(s -> {
            JiebaSegmenter segmenter = new JiebaSegmenter();
            List<SegToken> segs = segmenter.process(s, SegMode.SEARCH);
            return segs.stream().map(seg -> seg.word).collect(Collectors.toList()).iterator();
        });
    }

    public JavaPairRDD<String, Integer> wordCount(JavaRDD<String> data) {
        return data
            .mapToPair(s -> new Tuple2<>(s, 1))
            .reduceByKey((i1, i2) -> i1 + i2);
    }

    public List<Tuple2<String, Integer>> splitAndTopK(JavaRDD<String> text, int k) {
        JavaRDD<String> words = splitWords(text);
        return words
            .mapToPair(s -> new Tuple2<>(s, 1))
            .reduceByKey((i1, i2) -> i1 + i2)
            .top(k, new WordCountComparator());
    }
}

class WordCountComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    /* comparator for top K word frequency */
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        return Comparator.<Integer>naturalOrder().compare(o1._2(), o2._2());
    }    
}