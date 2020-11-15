package me.demo.service;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import scala.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

@Service
public class SparkService implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkService.class);

    @Autowired
    private transient JavaSparkContext sc;

    public JavaRDD<String> asRDD(List<String> text) {
        /* 把一个 string 的 list 转换成 rdd */
        return sc.parallelize(text);
    }
}
