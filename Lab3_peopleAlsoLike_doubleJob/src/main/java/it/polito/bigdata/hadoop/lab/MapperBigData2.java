package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab 3 people also like - Mapping at step 2
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type
	
	private TopKVector<WordCountWritable> localTopK;

	protected void setup(Context context) {
		localTopK = new TopKVector<WordCountWritable>(100);
	}
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		WordCountWritable currentPair = new WordCountWritable(key.toString(),
				Integer.valueOf(Integer.parseInt(value.toString())));
    		
    		localTopK.updateWithNewElement(currentPair);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(WordCountWritable w : localTopK.getLocalTopK()) {
    		context.write(NullWritable.get(), new WordCountWritable(w));
    	}
    }
}
