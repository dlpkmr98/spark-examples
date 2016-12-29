/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mycompany.sparkbasic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 *
 * @author dlpkmr98
 */
public class SparkHive {  
    
    public static void main(String[] args) {
        
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-common-2.2.0-bin-master\\");
        SparkConf conf= new SparkConf(true);
        conf.setMaster("local").setAppName("Spark Application...");
        JavaSparkContext jsc= new JavaSparkContext(conf);
        HiveContext sqlContext=new HiveContext(jsc.sc()); 
        sqlContext.sql("CREATE DATABASE temp");
//        sqlContext.sql("CREATE TABLE IF NOT EXISTS temp.persons (id INT,name STRING) "
//                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TextFile");
//        sqlContext.sql("LOAD DATA LOCAL INPATH 'C:\\Users\\dlpkmr98\\Desktop\\person.txt' INTO temp.TABLE persons");
//        DataFrame results = sqlContext.sql("FROM temp.persons SELECT *");
//        results.registerTempTable("persons"); 
//        results.show();
         sqlContext.sql("SHOW DATABASES").show();
    }
    
}
