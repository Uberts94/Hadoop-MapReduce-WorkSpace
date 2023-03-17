package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab 3 people also like - Reducing at step 2
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
	TopKVector<WordCountWritable> top100 = new TopKVector<WordCountWritable>(100);
	
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	for(WordCountWritable w : values) {
    		top100.updateWithNewElement(new WordCountWritable(w));
    	}
    	
    	for(WordCountWritable w : top100.getLocalTopK()) {
    		context.write(new Text(w.getWord()), new IntWritable(w.getCount()));
    	}
    }
}
