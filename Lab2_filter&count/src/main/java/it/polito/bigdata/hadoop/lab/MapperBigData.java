package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTERS;

/**
 * Lab2 filter and count
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
    		if(row[0].startsWith(prefix)) { 
    			context.getCounter(COUNTERS.MATCHED_PREFIX).increment(1);
    			context.write(new Text(row[0].toLowerCase()), new IntWritable(Integer.parseInt(row[1])));
    		} else context.getCounter(COUNTERS.NOT_MATCHED_PREFIX).increment(1);
    }
}
