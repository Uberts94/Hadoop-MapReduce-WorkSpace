package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Lab1 bonus track 
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");
            
            for(int i=0; i<words.length-1; i++) {
            	String bigram = words[i].toLowerCase().concat(" "+words[i+1].toLowerCase());
            	if(bigram.matches("[a-z0-0]+ [a-z0-0]+")) {
            		context.write(new Text(bigram), new IntWritable(1));
            	}
            }
    }
}
