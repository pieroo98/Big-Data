package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

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


		// Read the content of the input file register.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- station: integer (nullable = true)
		// |-- timestamp: timestamp (nullable = true)
		// |-- used_slots: integer (nullable = true)
		// |-- free_slots: integer (nullable = true)
		Dataset<Row> inputDF = ss.read().format("csv")
				.option("timestampFormat","yyyy-MM-dd HH:mm:ss")
				.option("delimiter", "\\t").option("header", true)
				.option("inferSchema", true).load(inputPath);

		// Cast the DataFrame to a typed Dataset<Readings>
		Dataset<Reading> inputDS = inputDF.as(Encoders.bean(Reading.class));

		// Remove the lines with #free slots=0 && #used slots=0
		// We use the typed version of the API
		Dataset<Reading> filteredDS = inputDS.filter(r -> (r.getFree_slots() != 0 || r.getUsed_slots() != 0));

		// Use the typed-map transformation to map each input record to a new
		// record with the following
		// schema
		// |-- station: integer (nullable = true)
		// |-- dayofweek: string (nullable = true)
		// |-- hour: integer (nullable = true)
		// |-- fullstatus: integer (nullable = true) - 1 = full, 0 = non-full

		Dataset<StationDayHourFull> stationWeekDayHourFullDS = filteredDS.map(record -> {

			StationDayHourFull newRecord = new StationDayHourFull();
			newRecord.setStation(record.getStation());
			newRecord.setDayofweek(DateTool.DayOfTheWeek(record.getTimestamp()));
			newRecord.setHour(DateTool.hour(record.getTimestamp()));

			if (record.getFree_slots() == 0)
				newRecord.setFullstatus(1);
			else
				newRecord.setFullstatus(0);

			return newRecord;

		}, Encoders.bean(StationDayHourFull.class));


		// Define one group for each combination "station, dayofweek, hour"
		RelationalGroupedDataset rgdstationWeekDayHourDS = stationWeekDayHourFullDS.groupBy("station", "dayofweek",
				"hour");

		// Compute the criticality for each group (station, dayofweek, hour),
		// i.e., for each pair (station, timeslot)
		// The criticality is equal to the average of fullStatus
		// Store the result in a typed Dataset<StationDayHourCriticality>
		// The schema is:
		// |-- station: integer (nullable = true)
		// |-- dayofweek: string (nullable = true)
		// |-- hour: integer (nullable = true)
		// |-- criticality: double (nullable = true)
		Dataset<StationDayHourCriticality> stationWeekDayHourCriticalityDS = rgdstationWeekDayHourDS
				.agg(avg("fullStatus")).withColumnRenamed("avg(fullStatus)", "criticality")
				.as(Encoders.bean(StationDayHourCriticality.class));


		// Select only the lines with criticality > threshold
		// Use a typed-filter transformation
		Dataset<StationDayHourCriticality> selectedPairsDS = stationWeekDayHourCriticalityDS
				.filter(record -> record.getCriticality() > threshold);


		// Read the content of the input file stations.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- id: integer (nullable = true)
		// |-- longitude: double (nullable = true)
		// |-- latitude: double (nullable = true)
		// |-- name: string (nullable = true)
		Dataset<Row> stationsDF = ss.read().format("csv").option("delimiter", "\\t").option("header", true)
				.option("inferSchema", true).load(inputPath2);

		// Cast the DataFrame to a typed Dataset<Readings>
		Dataset<Station> stationsDS = stationsDF.as(Encoders.bean(Station.class));


		// Join the selected critical "situations" with the stations table to
		// retrieve the coordinates of the stations.
		// Select only the column station, longitude, latitude and criticality
		// and sort records by criticality (desc), station (asc)
		// Cast the result to a typed Dataset<FinalRecords>
		// Schema

		Dataset<FinalRecord> selectedPairsIdCoordinatesCriticalityDS = selectedPairsDS
				.join(stationsDS, selectedPairsDS.col("station").equalTo(stationsDS.col("id")))
				.selectExpr("station", "dayofweek", "hour", "longitude", "latitude", "criticality")
				.sort(new Column("criticality").desc(), new Column("station"), new Column("dayofweek"),
						new Column("hour"))
				.as(Encoders.bean(FinalRecord.class));

		selectedPairsIdCoordinatesCriticalityDS.show();

		// Save the result in the output folder
		selectedPairsIdCoordinatesCriticalityDS.write().format("csv").option("header", true)
				.save(outputFolder);

		// Close the Spark session
		ss.stop();
	}
}
