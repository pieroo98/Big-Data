# Remove folders of the previous run
rm -rf out_Lab8

# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master local  target/Lab8_SolDS-1.0.0.jar sampleData/registerSample.csv sampleData/stations.csv 0.4 out_Lab8



