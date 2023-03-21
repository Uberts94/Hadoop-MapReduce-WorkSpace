package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key : UserId
                RatingWritable,    // Input value : RatingWritable containing UserId, Score, ProductId (redundancy)
                NullWritable,           // Output key type
                Text> {  // Output value : RatingWritable containing UserId, normalized score, ProductId (redundancy) 
	
	double sum;
	int ratingsCounter;
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<RatingWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	//assuming a small list of ratings
    	HashMap<String, Double> map = new HashMap<String, Double>(); //HashMap<ProductId,Score>
    	sum = 0;
    	ratingsCounter = 0;
    	
    	for(RatingWritable value : values) {
    		sum+=value.getScore();
    		ratingsCounter++;
    		map.put(value.getProduct(), value.getScore());
    	}
    	
    	map.forEach((prod, rating) -> {
    		try {
				context.write(NullWritable.get(), new Text(prod+","+key+","+(rating-(sum/ratingsCounter))));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	});
    }
}
