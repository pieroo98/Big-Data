package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	float max=0;
    	float vettore=0;
    	for(Text valori : values) {
    		vettore = vettore +Float.valueOf(valori.toString());
    		max = max +1;
    	}
    	float media=vettore/max;
    	context.write(key, new Text(String.valueOf(media)));
    }
}
