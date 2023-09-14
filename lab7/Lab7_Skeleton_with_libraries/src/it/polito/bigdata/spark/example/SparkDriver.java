package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		//leggo da input
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		//levo la prima riga del file che non mi serve
		JavaRDD<String> inputNoHeader = inputRDD.filter(line -> !line.startsWith("station"));
		
		//filtro le righe che hanno 0 e 0 come ultimi 2 parametri
		JavaRDD<String> inputFiltrato = inputNoHeader.filter(riga -> {
			String[] componenti = riga.split("\t");
			if(Integer.parseInt(componenti[2])==0 && Integer.parseInt(componenti[3])==0 )
				return false;
			else 
				return true;
		});
		
		DateTool data = new DateTool();
		//creo le coppie (chiave,valore) dove la chiave è formata da "IDstazione,Fri-15" mentre il valore sono le stazioni libere
		JavaPairRDD<String,Integer> primoFile = inputFiltrato.mapToPair(riga -> {
			String[] campiSingoli = riga.split("\t");
			String[] giornoEorario = campiSingoli[1].split(" ");
			String giorno = data.DayOfTheWeek(giornoEorario[0]);
			String[] ore = giornoEorario[1].split(":");
			return new Tuple2<String,Integer>(campiSingoli[0].concat(",").concat(giorno).concat("-").concat(ore[0]), Integer.parseInt(campiSingoli[3]) );
		});
		
		//raggruppo per chiave cosi ho la lista di elementi tutti insieme e posso calcolare il primo punto
		JavaPairRDD<String,Iterable<Integer>> primoFileRaggruppato = primoFile.groupByKey();
		
		//lascio la chiave uguale ma trasformo i valori in un unico valore Double, cioe quello richiesto
		JavaPairRDD<String,Double> primoFileCritical = primoFileRaggruppato.mapValues((Function<Iterable<Integer>, Double>) riga ->{
			Double conta= new Double(0);
			Double neutri = new Double(0);
			for(Integer tmp:riga) {
				if(tmp==0)
					neutri = neutri +1;
				conta = conta +1;
			}
			return neutri/conta;
		});
				
		//filtro per quelli con la criticita >= alla soglia
		JavaPairRDD<String,Double> sogliaMinima = primoFileCritical.filter(riga -> {
			if(riga._2()>=threshold)
				return true;
			else 
				return false;
		});
		
		//trasformo il mio RDD in Key = "IDstazione" , Value = "Fri-15-0.4"
		JavaPairRDD<String,String> sogliaMinKeyModify = sogliaMinima.mapToPair(riga -> {
			String[] parteUno = riga._1().split(",");
			
			return new Tuple2<String,String>(parteUno[0],parteUno[1].concat("-").concat(riga._2().toString()));
		});
		
		//uso la reduce per rimuovere le righe con la stessa IDstazione ma soglia piu piccola
				/*applico lo split al Valuee trovo nel seguente ordine:
				 * 
				 * riga1[0]="Fri" giorno
				 * riga1[1]="15" ora
				 * riga1[2]="0.4" criticita
				 * riga1[3]="2.453,40.853" coordinate
				 */
				JavaPairRDD<String,String> raggruppoKey = sogliaMinKeyModify.reduceByKey( (Value1, Value2) -> {
					String[] riga1 = Value1.split("-");
					String[] riga2 = Value2.split("-");
					//prima confronto solo le criticità
					if(Double.parseDouble(riga1[2])>Double.parseDouble(riga2[2]))
						return Value1;
					else if (Double.parseDouble(riga1[2])<Double.parseDouble(riga2[2]))
						return Value2;
					
					//al pari di criticità prendo l'orario piu piccolo
					else {
						if(Integer.parseInt(riga1[1])<Integer.parseInt(riga2[1]))
							return Value1;
						else if (Integer.parseInt(riga1[1])>Integer.parseInt(riga2[1]))
							return Value2;
						
						//al pari dell'orario prendo l'ordine alfabetico del giorno della settimana
						else {
							if(riga1[0].compareTo(riga2[0])<0)
								return Value1;
							else
								return Value2;
						}
					}
				} ); 
		
		JavaRDD<String> inputRDD2 = sc.textFile(inputPath2);
		JavaRDD<String> input2Filtrato = inputRDD2.filter(line -> !line.startsWith("id"));
		
		//creo le coppie (chiave,valore) senza alterare la struttura della riga solo per fare la join con l'IDstazione
		JavaPairRDD<String,String> secondoFile = input2Filtrato.mapToPair(riga -> {
			String[] campiSingoli = riga.split("\t");
			return new Tuple2<String,String>(campiSingoli[0], campiSingoli[1].concat(",").concat(campiSingoli[2]));
		});
		
		//faccio join
		JavaPairRDD<String,Tuple2<String,String>> risultatoJoin = raggruppoKey.join(secondoFile);
		
		
		/*spiego: riga._1() contiene IDstazione
		 * riga._2()._1() contiente il Value="Fri-15-0.4" dove l'ultimo double è la criticità
		 * riga._2()._2() contiente le coordinate unite dalla ","
		 */
		JavaPairRDD<String,String> modificaValue = risultatoJoin.mapToPair(riga -> {
			return new Tuple2<String,String>( riga._1(), riga._2()._1().concat("-").concat(riga._2()._2()) );
		});
		
		
		// creo una stringa unica dove ogni pezzo è separato dall'altro da uno spazio. Il formato finale sarà quello del pdf7
		JavaRDD<String> resultKML = modificaValue.map(riga -> {
			String[] pezzi = riga._2().split("-");
			return riga._1().concat(" ").concat(pezzi[0]).concat(" ").concat(pezzi[1]).concat(" ").concat(pezzi[2]).concat(" ").concat(pezzi[3]);
		});
		  
		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		resultKML.coalesce(1).saveAsTextFile(outputFolder); 

		// Close the Spark context
		sc.close();
	}
}
