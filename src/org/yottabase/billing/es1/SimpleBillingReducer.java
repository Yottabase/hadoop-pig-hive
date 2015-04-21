package org.yottabase.billing.es1;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleBillingReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private List<ProductAggregation> list = new LinkedList<ProductAggregation>();
	
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		Iterator<IntWritable> iter = values.iterator();
		
		Integer tot = new Integer(iter.next().get());
		
		//context.write(key, tot);
		
		//map.put(key.toString(), tot);
		list.add(new ProductAggregation(key.toString(), tot));
	}

	@Override
	protected void cleanup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		list.sort(new ProductAggregationComparator());
		
		for(ProductAggregation pa : list){
			context.write(new Text(pa.getProductName()), new IntWritable(pa.getCount()));
		}
	}
	
}
