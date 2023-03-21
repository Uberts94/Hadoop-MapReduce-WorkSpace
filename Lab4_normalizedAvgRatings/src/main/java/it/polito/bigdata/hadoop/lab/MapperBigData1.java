package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - mapping ratings
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    RatingWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String[] ratings = value.toString().split(",");
    		
    		if(!ratings[0].equals("Id")) {
    			//if this row is not the header one
    			context.write(new Text(ratings[2]), new RatingWritable(new String(ratings[2]),
    					Double.parseDouble(ratings[6]), new String(ratings[1])));
    		}
    }
}
