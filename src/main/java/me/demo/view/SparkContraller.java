package me.demo.view;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkContraller {
    @RequestMapping("/hello")
    public String hello() {
        return "Hello Spring Boot!";
    }
}