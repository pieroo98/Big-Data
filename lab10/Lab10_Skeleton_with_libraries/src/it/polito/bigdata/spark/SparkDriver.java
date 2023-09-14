package it.polito.bigdata.spark;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	public static void main(String[] args) throws InterruptedException {
		String outputPathPrefix;
		String inputFolder;

		inputFolder = args[0];
		outputPathPrefix = args[1];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Streaming Lab 10");
				
		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));		
		
		// Set the checkpoint folder (it is needed by some window transformations)
		jssc.checkpoint("checkpointfolder");

		JavaDStream<String> tweets = jssc.textFileStream(inputFolder);
		
		JavaDStream<String> tweets2 = tweets.map(riga -> riga.split("#")[1]);
		
		JavaPairDStream<String, Integer> wordsOnes = tweets2.mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));
		// TODO
		// Process the tweets JavaDStream.
		// Every time a new file is uploaded  in inputFolder a new batch of streaming data 
		// is generated 
		// ...
		JavaPairDStream<String, Integer> wordsCounts = wordsOnes.reduceByKeyAndWindow( (i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));

		wordsCounts.print();
		
		wordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");
		
		// Start the computation
		jssc.start();              
		
		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);
		
		jssc.close();
		
	}
}
