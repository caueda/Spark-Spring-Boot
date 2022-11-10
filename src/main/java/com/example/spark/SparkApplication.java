package com.example.spark;

import com.example.spark.config.SparkConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.*;

@SpringBootApplication(exclude = {org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class})
@Getter @Setter
@EnableConfigurationProperties(SparkConfig.class)
@Slf4j
public class SparkApplication implements CommandLineRunner {
    @Autowired
    private SparkConfig sparkConfig;
    @Autowired
    private ResourceLoader resourceLoader;

    public static void main(String[] args) {
        SpringApplication.run(SparkApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("folder {}", sparkConfig.getFolder());
        process();
    }


    public void process() throws IOException {
//        var var1 = System.getProperty("var1");
//        var var2 = System.getProperty("var2");

        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        SparkSession spark = SparkSession.builder()
                .appName("App")
                .master("local[*]")
                .getOrCreate();

//        SparkConf conf = new SparkConf().setAppName("App"); //.setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<Double> myRdd = sc.parallelize(inputData);
        JavaRDD<Double> myRdd = spark.createDataset(inputData, Encoders.DOUBLE()).javaRDD();

        var result = myRdd.reduce((aDouble, aDouble2) -> aDouble + aDouble2);
        var stream = new PathMatchingResourcePatternResolver(resourceLoader)
                .getResource("classpath:transform.yml")
                .getInputStream();
        Map<String, String> map = new ObjectMapper(new YAMLFactory()).readValue(stream, LinkedHashMap.class);
        System.out.println(map.get("user"));
        stream.close();

        stream = new PathMatchingResourcePatternResolver(resourceLoader)
                .getResource("file:c:/apps/transform.yml")
                .getInputStream();
        map = new ObjectMapper(new YAMLFactory()).readValue(stream, LinkedHashMap.class);
        System.out.println(map.get("user"));
        stream.close();

        if(new PathMatchingResourcePatternResolver(resourceLoader)
                .getResource("file:c:/apps/transform_not_exist.yml").getFile().exists()) {
            System.out.println("File transform_not_exist.yml exists.");
        } else {
            System.out.println("File transform_not_exist.yml NOT exists.");
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(">>>>> Result: " + result + "    Time: " + new Date().toString());
        System.out.println("var1=" + sparkConfig.getVar1() + "    var2=" + sparkConfig.getVar2());
        System.out.println("externalLocation=" + sparkConfig.getExternalLocation() );
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        spark.close();
    }
}
