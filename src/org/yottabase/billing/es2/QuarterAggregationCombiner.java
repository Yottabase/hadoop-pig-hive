package org.yottabase.billing.es2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuarterAggregationCombiner extends
		Reducer<Text, CountByMounth, Text, CountByMounth> {
	
	
	public void reduce(Text key, Iterable<CountByMounth> values, Context context) 
			throws IOException, InterruptedException {
		
		Map<Integer, Integer> groupByMounth = new HashMap<Integer, Integer>();
		
		for(CountByMounth cm : values){
			Integer count = groupByMounth.remove(cm.getMounth());
			
			if(count == null){
				count = cm.getCount();
			}else{
				count = cm.getCount() + count;
			}
			
			groupByMounth.put(cm.getMounth(), count);
		}
		
		for(Entry<Integer, Integer> entry: groupByMounth.entrySet()){
			
			context.write(key, new CountByMounth(entry.getKey(), entry.getValue()));
			
		}
		
	}
			
}
