package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab 3 People also like...
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String[] words = value.toString().split(",");
    		
    		for(int i = 1; words.length > 2 && i < words.length; i++) {
    			for(int j = i+1; j < words.length; j++) {
    				if(words[i].compareTo(words[j]) < 0) {
    					context.write(new Text(words[i]+"-"+words[j]), new IntWritable(1));
    				}
    			}
    		}
    }
}
