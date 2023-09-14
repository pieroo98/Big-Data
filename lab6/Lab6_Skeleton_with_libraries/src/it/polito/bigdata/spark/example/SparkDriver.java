package it.polito.bigdata.spark.example;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {
		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the cluster
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		JavaRDD<String> inputRDD2 = inputRDD.filter(line -> !line.startsWith("Id,"));
		// TODO .......
		
		//formo le coppie (IDutente, prodotto)
		JavaPairRDD<String,String> coppieRDD = inputRDD2.mapToPair(riga -> {
			if(riga.split(",")[2].startsWith("A") && riga.split(",")[1].startsWith("B"))
				return new Tuple2<String,String>( riga.split(",")[2], riga.split(",")[1]);
			else if (riga.split(",")[2].startsWith("A") ==false && riga.split(",")[1].startsWith("B"))
				return new Tuple2<String,String>("null", riga.split(",")[1]);
			else if (riga.split(",")[2].startsWith("A") && riga.split(",")[1].startsWith("B")==false)
				return new Tuple2<String,String>(riga.split(",")[2],"null");
			else 
				return new Tuple2<String,String>("null","null");
						}).distinct();
		
		//raggruppo per chiave IDutente e metto tutti i prodotti acquistati in un vettore (sarebbe Iterable<String>)
		JavaPairRDD<String,Iterable<String>> RaccoltaValueRDD = coppieRDD.groupByKey();
		
		//trasformo le coppie in ("IDutente", "prod1 prod2") iterando e formando tutte le coppie possibile per ogni utente
		JavaPairRDD<String,String> tempRDD = RaccoltaValueRDD.flatMapValues(riga -> 
			{List<String> prodottiCoppie = new ArrayList<>(); 
			for(String prod1 : riga) {
				for(String prod2 : riga) {
					if(prod1.compareTo(prod2)<0 && (prod1.compareTo("null")!=0 && prod2.compareTo("null")!=0)) {
						String tmp = prod1.concat(" ").concat(prod2);
						
						  boolean trovato = false; 
						  for(int i=0;i<prodottiCoppie.size();i++) {
							  if(prodottiCoppie.get(i).contains(tmp)==true) 
								  trovato = true;
						  } 
						  if (trovato == false)
							prodottiCoppie.add(tmp);
					}
					//questo blocco non serve, ci generava i doppioni. prova esempio con A,B,C,D come vettore da generare le coppie.
					/*else if (prod1.compareTo(prod2)>0 && (prod1.compareTo("null")!=0 && prod2.compareTo("null")!=0)){
						String tmp = prod2.concat(" ").concat(prod1);
						
						  boolean trovato = false; 
						  for(int i=0;i<prodottiCoppie.size();i++) {
						  if(prodottiCoppie.get(i).contains(tmp)==true) trovato = true; } 
						  if (trovato == false)
							  prodottiCoppie.add(tmp);
					}*/
				}
			}
			return prodottiCoppie;
			});
		
		//estrapolo solo le coppie di prodotti
		JavaRDD<String> prodottiFiltrati = tempRDD.values();
		
		//genero le coppie con frequenza uguale a 1
		JavaPairRDD<String,Integer> prodottiFreq = prodottiFiltrati.mapToPair(riga -> new Tuple2<String,Integer>(riga,1));
		
		//in questo modo ho i prod come chiave ripetuti sono 1 volta e un iterable di 1 che sono le frequenze, ora devo sommarli.
		JavaPairRDD<String,Iterable<Integer>> prodottiRaggruppati = prodottiFreq.groupByKey();
		
		//sommo le frequenze di 1
		JavaPairRDD<String,Integer> prodottiFreqReal = prodottiRaggruppati.mapValues( numeri -> 
		{int count=0;
		for(Integer numero: numeri) {
			count= count + numero;
		}
		return new Integer(count);
		});
		
		//filtro solo quelli con frequenza > 1
		JavaPairRDD<String,Integer> ProdFreqFinal = prodottiFreqReal.filter(prova -> 
		{
			if(prova._2()>1)
				return true;
			else 
				return false;
		});
		
		//scambio (key,value). Quindi ora la Frequenza è la chiave e la coppia di prod è il value.
		JavaPairRDD <Integer,String> ProdInvertiti = ProdFreqFinal.mapToPair( (PairFunction<Tuple2<String, Integer>, Integer, String>) riga -> 
			{ return new Tuple2<Integer,String>(riga._2(),riga._1());
			}).sortByKey(false);
		
		//ssalvo nella cartella
		ProdInvertiti.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
