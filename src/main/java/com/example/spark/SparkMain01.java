package com.example.spark;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@Getter @Setter
@Slf4j
@EnableConfigurationProperties
@ConfigurationProperties
public class SparkMain01 {

    private String configFolder;

    public static void main(String[] args) {
        log.info("configFolder {}", new SparkMain01().getConfigFolder());
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        SparkConf conf = new SparkConf().setAppName("App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        var result = myRdd.reduce((aDouble, aDouble2) -> aDouble + aDouble2);

        System.out.println("Result: " + result);

        //sc.close();
    }
}
