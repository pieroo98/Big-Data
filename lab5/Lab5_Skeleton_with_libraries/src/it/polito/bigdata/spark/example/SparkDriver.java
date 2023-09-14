package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {
	public static void main(String[] args) {
		String inputPath;
		String outputPath;
		String prefix;
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		/*
		 * Task 1
		*/
		JavaRDD<String> words = wordFreqRDD.filter(
			single -> {
			if(single.startsWith(prefix) )
				return true;
			else
				return false;
			});
		
		long num_max_righe = words.count();
		System.out.println(num_max_righe);
		
		
		JavaRDD<Integer> words_freq = words.map(word -> Integer.parseInt(word.split("\t")[1]));
		long valore_max = words_freq.reduce((a,b) -> {if(a>b) return a;
													  else {return b;}});
		
		System.out.println(valore_max);
		/*
		 * Task 2
		 */

		JavaRDD<String> words_freq_80_perc = words.filter(
				word -> {
				if(Integer.parseInt(word.split("\t")[1])>=0.8*valore_max )
					return true;
				else
					return false;
				});
		long num_max_righe_2 = words_freq_80_perc.count();
		System.out.println(num_max_righe_2);
		JavaRDD<String> words_no_freq = words_freq_80_perc.map( word -> word.split("\t")[0]);
		words_no_freq.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
