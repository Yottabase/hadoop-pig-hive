package org.yottabase.billing.optional.es1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupByProductReducer extends
		Reducer<Text, ProductCount, Text, Text> {
	
	public void reduce(Text key, Iterable<ProductCount> values,
			Context context) throws IOException, InterruptedException {
		
		String out = "";
		
		for(ProductCount productPair : values){
			out += productPair.toString() + "\t";
		}
		
		context.write(key, new Text(out));
	}	
}
