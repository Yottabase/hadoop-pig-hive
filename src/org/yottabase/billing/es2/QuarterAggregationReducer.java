package org.yottabase.billing.es2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class QuarterAggregationReducer extends
		Reducer<ProductByMounth, IntWritable, ProductByMounth, IntWritable> {
	
	
	public void reduce(ProductByMounth key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		int tot = 0;
		
		for(IntWritable partialCount : values){
			tot += partialCount.get();
		}
		
		context.write(key, new IntWritable(tot));
	}
			
}
