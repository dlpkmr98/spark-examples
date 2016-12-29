/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mycompany.sparkbasic;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author dlpkmr98
 */
public class SparkBasic {
    
    public static void main(String[] args) {
        
        SparkConf conf= new SparkConf(true);
        conf.setMaster("local").setAppName("Spark Application...");
        JavaSparkContext jsc= new JavaSparkContext(conf);
        List lst = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd1=jsc.parallelize(lst);
        JavaRDD<Integer> rdd2=rdd1.map(new Function<Integer, Integer>(){

            @Override
            public Integer call(Integer t1) throws Exception {
                return t1*t1;
            }          
            
        });
        
        System.out.println("element..."+rdd2.collect());
    }
   
    
}
