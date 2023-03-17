package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                NullWritable,           // Output key type
                Vector<WordCountWritable>> {  // Output value type
	TopKVector<WordCountWritable> top3 = new TopKVector<WordCountWritable>(100);
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	int occurrences = 0;
    	for(IntWritable value : values) occurrences += value.get();
    	top3.updateWithNewElement(new WordCountWritable(new String(key.toString()), new Integer(occurrences)));
    }
    
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	System.out.println(top3.getLocalTopK());
    	context.write(null, new Vector<WordCountWritable>(top3.getLocalTopK()));
    }
}
