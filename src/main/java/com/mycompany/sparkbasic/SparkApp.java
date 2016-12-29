/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkbasic;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author dlpkmr98
 */
public class SparkApp {
    
    
    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf(true);
        conf.setAppName("Application first").setMaster("local");
        JavaSparkContext jsc= new JavaSparkContext(conf);
//        Integer data[]={1,2,3,4,5};
//        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(data));
//        
//        
//        
//        JavaRDD<Integer> rdd1=rdd.map(new Function<Integer,Integer>(){
//            @Override
//            public Integer call(Integer t1) throws Exception {
//                return t1*t1;
//            }
//            
//        }).filter(new Function<Integer, Boolean>(){
//            @Override
//            public Boolean call(Integer t1) throws Exception {
//               if(t1==4){
//                   return false;
//               }else{
//                   return true;
//               }
//            }
//            
//            
//        });
//        
//        System.out.println("result+++++"+rdd1.collect());

   JavaRDD<String> rdd=jsc.textFile("C:\\Users\\dlpkmr98\\Desktop\\person.txt");
   JavaRDD<String> rdd1=rdd.flatMap(new FlatMapFunction<String,String>(){
            @Override
            public Iterable<String> call(String t) throws Exception {
                String d[]=t.split(",");
                return Arrays.asList(d);
            }
       
       
   });
   
        JavaPairRDD<String,Integer> rdd2=rdd1.mapToPair(new PairFunction<String, String,Integer>(){
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2(t,1);
            }
            
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
               return t1+t2;
            }
        });
        
        //rdd2.saveAsTextFile(path);
        
        System.out.println("elements..."+rdd2.collect());    
    }
    
    
}
