/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkbasic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author dlpkmr98
 */
public class SparkSubtractByKey {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf(true);
        conf.setAppName("Spark Application").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> oldrdd = jsc.textFile("C:\\Users\\dlpkmr98\\Desktop\\old.txt");
        JavaRDD<String> newrdd = jsc.textFile("C:\\Users\\dlpkmr98\\Desktop\\new.txt");
        JavaPairRDD<Integer, String> oldpyrdd = oldrdd.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String x) {
                String s[] = x.split(",");
                return new Tuple2(Integer.parseInt(s[0]),x.substring(2));
            }
        });
        JavaPairRDD<Integer, String> newpyrdd = newrdd.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String x) {
                String s[] = x.split(",");
                return new Tuple2(Integer.parseInt(s[0]),x.substring(2));
            }
        });
        
        JavaPairRDD<Integer, String> subpyrdd=oldpyrdd.subtractByKey(oldpyrdd);

        System.out.println("olddata++++++++++++" + subpyrdd.collect());

    }

}
