package me.demo.view;

import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import me.demo.operator.TextOperators;
import me.demo.service.SparkService;

@RestController
public class SparkContraller {
    @Autowired
    private SparkService sparkService;
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