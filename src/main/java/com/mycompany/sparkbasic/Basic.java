/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkbasic;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author dlpkmr98
 */
public class Basic {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\Users\\Downloads\\");
        SparkConf conf = new SparkConf(true);
        conf.setAppName("Basic SparkApp").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);
//SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(jsc.sc());
        HiveContext hqlContext = new org.apache.spark.sql.hive.HiveContext(jsc.sc());
//Load a text file and convert each line to a JavaBean.
// JavaRDD<Person> people = jsc.textFile("C:\\Users\\Desktop\\person.txt").map(
// new Function<String, Person>() {
// public Person call(String line) throws Exception {
// String[] parts = line.split(" ");
// Person person = new Person();
// person.setName(parts[0]);
// person.setAge(Integer.parseInt(parts[1].trim()));
// return person;
// }
// });
// // Apply a schema to an RDD of JavaBeans and register it as a table.
// DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
// schemaPeople.registerTempTable("person");

// SQL can be run over RDDs that have been registered as tables.
// DataFrame teenagers = sqlContext.sql("SELECT name FROM person WHERE age >= 13 AND age <= 26");
//teenagers.printSchema();
//teenagers.select("name").show();
//teenagers.cache();
// JavaRDD<String> list=teenagers.javaRDD().map(new Function<Row,String>(){
// public String call(Row row) {
// return "Name: " + row.getString(0);
// }
// });
        sqlContext.sql("DROP TABLE IF EXISTS person");
        sqlContext.sql("CREATE TABLE IF NOT EXISTS person (name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘\\t’ STORED AS TextFile");
        sqlContext.sql("LOAD DATA LOCAL INPATH ‘C:\\Users\\Desktop\\person.txt’ INTO TABLE person");

// HashMap<String, String> saveOptions = new HashMap<String, String>();
// saveOptions.put("header", "true");
// saveOptions.put("path", "newcars.csv");
// saveOptions.put("codec", "org.apache.hadoop.io.compress.GzipCodec");
        DataFrame results = sqlContext.sql("FROM person SELECT *");
//results.registerTempTable("abc"); //they would cache the data in optimized in-memory columnar format similar to cacheTable
//// mark this dataframe to be stored in memory once evaluated
//DataFrame memResult = results.cache(); //would cache it just as any other RDD in row-oriented fashion, cache is simmilar to persist(storagelevel.memory_only)
// mark this dataframe to be broadcast
//Broadcast<DataFrame> broadcastedFieldNames = jsc.broadcast(memResult);
//DataFrame memResult1 = broadcastedFieldNames.value();
//memResult1.show();
//DataFrame teenagers = sqlContext.sql("SELECT * FROM abc WHERE age >= 13 AND age <= 19");
//teenagers.select("name", "age").show();
//sqlContext.sql("CREATE TABLE IF NOT EXISTS person1 (name STRING, age INT)");
//teenagers.saveAsTable("person1", SaveMode.Append);
//DataFrame results1 = sqlContext.sql("FROM person1 SELECT name");
//results1.select("name").show();
// sqlContext.sql("show tables").show();
//sqlContext.tableNames();
//sqlContext.sql("set spark.sql.hive.version").show();
//sqlContext.sql("describe database extended default").show();
//DataFrame columnCollection = sqlContext.sql("from TABLE SELECT collect_list(Column) as columnCollection ");
        results.show();
        String schemaString = "name age";
        List<StructField> fields = new ArrayList<StructField>();
        String data[] = schemaString.split(" ");
        fields.add(createStructField(data[0], StringType, true));
        fields.add(createStructField(data[1], IntegerType, true));
        StructType schema = createStructType(fields);
        JavaRDD<Row> rowrdd = jsc.emptyRDD();
        DataFrame personFrame = sqlContext.createDataFrame(rowrdd, schema);
        personFrame.registerTempTable("persondata");
//personFrame.saveAsTable("mannagedtable1", SaveMode.Ignore);
        sqlContext.sql("FROM persondata SELECT *").show();
        sqlContext.sql("INSERT OVERWRITE TABLE persondata SELECT name, age FROM person").show();

        DataFrame tmp_unq_frame = hqlContext.sql("SELECT ………");

        String schemaString1 = "unique_call_id,session_id,geotel_id,min_gmt_date_time,cs_updt_dt,min_gmt_date";
        List<StructField> fields1 = new ArrayList<StructField>();
        String data1[] = schemaString1.split(",");
        fields.add(createStructField(data1[0], StringType, true));
        fields.add(createStructField(data1[1], StringType, true));
        fields.add(createStructField(data1[2], StringType, true));
        fields.add(createStructField(data1[3], TimestampType, true));
        fields.add(createStructField(data1[4], TimestampType, true));
        fields.add(createStructField(data1[5], DateType, true));
        StructType schema1 = createStructType(fields1);
        DataFrame tmp_unq_Frame = hqlContext.createDataFrame(tmp_unq_frame.rdd(), schema1);

        tmp_unq_Frame.show(2);

// tmp_unq_Frame.registerTempTable("tmp_unq");
//sqlContext.sql("FROM tmp_unq SELECT * LIMIT 2").show();
//
    }

}
