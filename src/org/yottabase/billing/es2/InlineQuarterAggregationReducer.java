package org.yottabase.billing.es2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InlineQuarterAggregationReducer extends
		Reducer<Text, CountByMounth, Text, Text> {
	
	
	public void reduce(Text key, Iterable<CountByMounth> values, Context context) 
			throws IOException, InterruptedException {
		
		String out = "";
		for(CountByMounth cm : values){
			out += cm.toString() + " ";
		}	
		
		out = out.trim();
		
		context.write(key, new Text(out));
	}
			
}
