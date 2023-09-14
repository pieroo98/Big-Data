package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.StopWordsRemover;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPath;

		inputPath=args[0];
	
		
		// Create a Spark Session object and set the name of the application
		// We use some Spark SQL transformation in this program
		SparkSession ss = SparkSession.builder().appName("Spark Lab9").getOrCreate();

		// Create a Java Spark Context from the Spark Session
		// When a Spark Session has already been defined this method 
		// is used to create the Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

		
		
    	//EX 1: READ AND FILTER THE DATASET AND STORE IT INTO A DATAFRAME
		
		// Read data
		JavaRDD<String> data=sc.textFile(inputPath);
		
		// To avoid parsing the comma escaped within quotes, you can use the following regex:
		// line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		// instead of the simpler
		// line.split(",");
		// this will ignore the commas followed by an odd number of quotes.

		// Remove header and reviews with HelpfulnessDenominator equal to 0
		JavaRDD<String> labeledData = data.filter( riga -> {
			String[] elementiRiga = riga.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if(elementiRiga[0].equals("Id"))
				return false;
			else {
				Integer val = Integer.parseInt(elementiRiga[5]); 
				if (val == 0 )
					return false;
				else 
					return true;
				}
		}); 

		Tokenizer tokenizer = new Tokenizer();
		// Map each element (each line of the input file) to a LabelPoint
		// Decide which numerical attributes you want to insert into features (data type: Vector) 
		JavaRDD<LabeledDocument> dataRDD=labeledData.map( riga -> {
			String[] elementiRiga = riga.split(",");
			Double etichetta = new Double(0);
			Double lunghezzaTesto = new Double(0);
			etichetta = Double.valueOf(elementiRiga[4])/Double.valueOf(elementiRiga[5]);
			if(etichetta>=0.9)
				etichetta = 1.0;
			else 
				etichetta = 0.0;
			lunghezzaTesto = (double) elementiRiga[9].length();
			String text = elementiRiga[8];
			return new LabeledDocument(etichetta, text);
			//return new LabeledPoint(etichetta, Vectors.dense(lunghezzaTesto,Double.parseDouble(elementiRiga[6])));
		}); 
		
		// Define a Dataframe, with columns label and features, from dataRDD 
		Dataset<Row> schemaReviews = ss.createDataFrame(dataRDD, LabeledDocument.class).cache();
		
		tokenizer.setInputCol("text").setOutputCol("Summary2");
		StopWordsRemover remover = new StopWordsRemover().setInputCol("Summary2").setOutputCol("filteredWords");
		HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("filteredWords").setOutputCol("rawFeatures");
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");


		// Debug 
		// Display 5 example rows.
    	schemaReviews.show(5);
		
        // The following part of the code splits the data into training 
		// and test sets (30% held out for testing)
        Dataset<Row>[] splits = schemaReviews.randomSplit(new double[]{0.7, 0.3},5);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        //creo l'oggetto LogisticRegression
        LogisticRegression lr = new LogisticRegression();
        
    	//EX 2: CREATE THE PIPELINE THAT IS USED TO BUILD THE CLASSIFICATION MODEL
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, remover, hashingTF, idf, lr});
        
        // Train model. Use the training set 
        PipelineModel model = pipeline.fit(trainingData);
		

		/*==== EVALUATION ====*/
		
		// Make predictions for the test set.
		Dataset<Row> predictions = model.transform(testData);

		// Select example rows to display.
		predictions.show(5);

		// Retrieve the quality metrics. 
        MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

        double accuracy = metrics.accuracy();
		System.out.println("Accuracy = " + accuracy);
            
        // Close the Spark context
		sc.close();
	}
}
