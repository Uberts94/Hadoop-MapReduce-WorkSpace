package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Lab2_filteringDB
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
	String prefix;
	
	protected void setup(Context context) {
		prefix = context.getConfiguration().get("prefix");
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		String[] row = value.toString().split("\\t");
    		if(row[0].startsWith(prefix)) 
    			context.write(new Text(row[0].toLowerCase()), new IntWritable(Integer.parseInt(row[1]))) ;
    }
}
