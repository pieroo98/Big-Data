package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

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

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		Dataset<InfoStazione> inputDF = ss.read().format("csv").option("delimiter", "\\t").option("timestampFormat","yyyy-MM-dd HH:mm:ss")
				.option("header", true).option("inferSchema", true).load(inputPath).as(Encoders.bean(InfoStazione.class));
				 				
		//faccio il filtro sugli slot liberi e occupati diversi da zero
		Dataset<InfoStazione> dsFiltrati = inputDF.filter(oggettoInfoStazione -> {
			if(oggettoInfoStazione.getFree_slots()==0 && oggettoInfoStazione.getUsed_slots() == 0)
				return false;
			else
				return true;
		});
		
		// Define a Dataset of InfoStazione2 objects from the DataFrame
		Dataset<InfoStazione2> dsGiornoEora = dsFiltrati.map( oggettoInfoStazione2 -> {
			
			InfoStazione2 newInfoStazione2 = new InfoStazione2();
			
			String[] giornoEora = oggettoInfoStazione2.getTimestamp().toString().split(" "); 
			
			int val = oggettoInfoStazione2.getFree_slots();
			if(val==0)
				val=1;
			else 
				val=0;
			
			newInfoStazione2.setStation(oggettoInfoStazione2.getStation());
			newInfoStazione2.setGiornoEora(DateTool.DayOfTheWeek(giornoEora[0]).concat("-").concat(giornoEora[1].split(":")[0]));
			newInfoStazione2.setFree_slots(val);
			return newInfoStazione2; }
			,Encoders.bean(InfoStazione2.class));
		
		//raggruppo per idStazione e giornoEora e calcolo media della criticita
		Dataset<InfoStazione3> dsCriticita = dsGiornoEora.groupBy("station","GiornoEora").avg("free_slots").withColumnRenamed("avg(free_slots)", "criticita")
				.as(Encoders.bean(InfoStazione3.class));
		
		//filtro per criticita >= la soglia
		Dataset<InfoStazione3> criticitaSopraSoglia = dsCriticita.filter(oggettoInfoStazione3 -> {
			if(oggettoInfoStazione3.getCriticita()>=threshold)
				return true;
			else
				return false;
		});
		
		//leggo secondo File
		Dataset<CoordinateStazione> inputDF2 = ss.read().format("csv").option("delimiter", "\\t").option("header", true).option("inferSchema", true)
				.load(inputPath2).as(Encoders.bean(CoordinateStazione.class));
						
		//levo l'ultimo attributo, cioe il nome che non mi serve
		Dataset<CoordinateStazione2> coordinateStaz2 = inputDF2.map(oggettoCoordinate -> {
			CoordinateStazione2 tmp = new CoordinateStazione2();
			tmp.setId(oggettoCoordinate.getId());
			tmp.setLatitude(oggettoCoordinate.getLatitude());
			tmp.setLongitude(oggettoCoordinate.getLongitude());
			return tmp;
		},Encoders.bean(CoordinateStazione2.class));
		
		//faccio il join tra i due DataFrame
		Dataset<TabelleUnite> TabelleJoin = criticitaSopraSoglia.join(coordinateStaz2, criticitaSopraSoglia.col("station").equalTo(coordinateStaz2.col("id")))
				.selectExpr("station", "giornoEora", "longitude", "latitude", "criticita")
				.sort(new Column("criticita").desc(), new Column("station"), new Column("giornoEora")).as(Encoders.bean(TabelleUnite.class));
		
		//salvo
		TabelleJoin.write().format("csv").option("header", true).save(outputFolder);
		
		// Close the Spark session
		ss.stop();
		
	}
}
