package me.demo.bean;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Configuration
@ConfigurationProperties(prefix = "spark")
public class SparkContextBean {
    private String sparkHome = ".";

    private String appName = "WordFrequencyStatistic";

    private String master = "local[*]";

    @Bean
    @ConditionalOnMissingBean(SparkConf.class)
    public SparkConf sparkConf() {
	   return new SparkConf().setAppName(appName).setMaster(master);
    }

    @Bean
    @ConditionalOnMissingBean(JavaSparkContext.class)
    public JavaSparkContext javaSparkContext() {
	   return new JavaSparkContext(sparkConf());
    }

    public String getSparkHome() {
	  return sparkHome;
    }

    public void setSparkHome(String sparkHome) {
	   this.sparkHome = sparkHome;
    }

    public String getAppName() {
	   return appName;
    }

    public void setAppName(String appName) {
	   this.appName = appName;
    }

    public String getMaster() {
	   return master;
    }

    public void setMaster(String master) {
	   this.master = master;
    }
}
