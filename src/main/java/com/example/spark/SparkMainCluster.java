package com.example.spark;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import scala.Serializable;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Getter @Setter
@Slf4j
public class SparkMainCluster implements Serializable {

    public static void main(String[] args) {
        var var1 = System.getProperty("var1");
        var var2 = System.getProperty("var2");

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

        var csv = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(SparkMainCluster.class.getClassLoader().getResource("users.csv").getPath().toString());

        csv.createOrReplaceTempView("user");

        spark.sql("select * from user").show();

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(">>>>> Result: " + result + "    Time: " + new Date().toString());
        System.out.println("var1=" + var1 + "    var2=" + var2);
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        spark.close();
    }
}
