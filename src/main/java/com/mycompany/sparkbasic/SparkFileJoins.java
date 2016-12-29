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
public class SparkFileJoins {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf(true);
        conf.setAppName("Application first").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rddfile1 = jsc.textFile("C:\\Users\\dlpkmr98\\Desktop\\file2.txt");
        JavaRDD<String> rddfile2 = jsc.textFile("C:\\Users\\dlpkmr98\\Desktop\\file3.txt");

        JavaPairRDD<Integer, String> rddfile1Map = rddfile1.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String x) {
                String s[] = x.split(",");
                return new Tuple2(Integer.parseInt(s[2]), x);
            }
        });

        JavaPairRDD<Integer, String> rddfile2Map = rddfile2.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String x) {
                String s[] = x.split(",");
                return new Tuple2(Integer.parseInt(s[2]), x);
            }
        });

        JavaPairRDD<Integer, String> rddfileJoin = rddfile1Map.join(rddfile2Map).
                mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, String>>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<Integer, Tuple2<String, String>> t) throws Exception {
                        return new Tuple2(t._1, t._2._1);
                    }
                });
        
          JavaPairRDD<Integer, String> rddfileJoinValues = rddfileJoin.values().mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String x) {
                String s[] = x.split(",");
                return new Tuple2(Integer.parseInt(s[3]), x);
            }
        }).sortByKey(true);
//       
//        });

        System.out.println("data+++++" + rddfileJoinValues.values().collect());
        //System.out.println("data+++++" + rddfile2Map.collect());

    }

}
