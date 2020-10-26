package me.demo.view;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import me.demo.service.SparkService;

@RestController
public class SparkContraller {
    @Autowired
    private transient SparkService sparkService;

    @RequestMapping("/hello")
    public String hello() {
        return "Hello Spring Boot!";
    }

    String[] sentences = new String[] {
        "这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。",
        "我不喜欢日本和服。",
        "雷猴回归人间。",
        "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作", 
        "结过婚的和尚未结过婚的" 
    }; 

    @RequestMapping("/test")
    public String test() {
        System.out.println(sentences);
        System.out.println(sparkService);
        List<String> texts = Arrays.asList(sentences);
        return sparkService.topK(texts, 10).toString();
    }
}