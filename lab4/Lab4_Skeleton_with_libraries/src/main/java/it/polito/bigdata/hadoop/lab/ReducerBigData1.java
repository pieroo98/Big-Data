package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	HashMap<String,Integer> accumu= new HashMap();
    	
		/* Implement the reduce method */
    	float max=0;
    	float vettore=0;
    	for(Text valori : values) {
    		String[] comp = valori.toString().split(",");
    		accumu.put(comp[0], Integer.parseInt(comp[1]));
    		vettore = vettore +Integer.parseInt(comp[1]);
    		max = max +1;
    	}
    	float media=vettore/max;
    	for(Entry<String, Integer> valori : accumu.entrySet()) {    		
    		String due = valori.getKey();
    		float n_val = valori.getValue();
    		float real = n_val - media;
    		String finale = due.concat(",").concat(String.valueOf(real));
    		context.write(new Text(key.toString().concat(",")), new Text(finale));
    	}
    }
}
