package me.demo.view;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import me.demo.operator.TextOperators;
import me.demo.service.SparkService;
import scala.Tuple2;

@RestController
public class SparkContraller implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private static final transient Logger logger = LoggerFactory.getLogger(SparkService.class);

    @Autowired
    private transient SparkService sparkService;

    private TextOperators textOpertors = new TextOperators();

    @RequestMapping("/test")
    public List<Tuple2<String, Integer>> test(
        @RequestParam String text,
        @RequestParam(defaultValue = "10") String top
    ) {
        List<String> lines = new ArrayList<>(Arrays.asList(text));
        return textOpertors.splitAndTopK(sparkService.asRDD(lines), Integer.parseInt(top));
    }
}