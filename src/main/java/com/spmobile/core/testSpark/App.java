package com.spmobile.core.testSpark;



import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        SparkConf confSpark = new SparkConf().setMaster("local").setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(confSpark);
        
        JavaRDD<String> input = sc.textFile("/Users/diegobenavides/Documentos/Scala-Works/WorkCount/in.txt");
        
        JavaRDD<String> words = input.flatMap(
        		new FlatMapFunction<String, String>() {
        		    public Iterator<String> call(String x) {
        		      return Arrays.asList(x.split(" ")).iterator();
        		    }
        		}
        		);
        	// Transform into pairs and count.
        	/*JavaPairRDD<String, Integer> counts = words.mapToPair(
        		  new PairFunction<String, String, Integer>(){
        		    public Tuple2<String, Integer> call(String x){
        		      return new Tuple2(x, 1);
        		    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
        		          public Integer call(Integer x, Integer y){ return x + y;}
        		    });*/
         JavaPairRDD<String, Integer> counts = words.mapToPair(
      		  new PairFunction<String, String, Integer>(){
      		    public Tuple2<String, Integer> call(String x){
      		      return new Tuple2(x, 1);
      		    }});
         
         Map<String,Long> val = counts.countByKey();
         
         for(Map.Entry<String,Long> entry: val.entrySet()) {
        	 	System.out.println(entry.getKey() + ", " + entry.getValue());
         }
         
        	// Save the word count back out to a text file, causing evaluation.
        	// counts.saveAsTextFile("/Users/diegobenavides/Documentos/Scala-Works/WorkCount/out");
        
        
    }
}
