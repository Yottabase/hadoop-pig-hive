package org.yottabase.billing.es1;

import java.io.IOException;
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
		
		int tot = 0;
		
		for(IntWritable count : values){
			tot += count.get();
		}
		
		//nota: new Text(key) è necessario visto che hadoop non ricrea key, ma semplicemente lo aggiorna
		list.add(new ProductAggregation(new Text(key), new IntWritable(tot)));
	}

	@Override
	protected void cleanup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		list.sort(new ProductAggregationComparator());
		
		for(ProductAggregation pa : list){
			context.write(pa.getProductName(), pa.getCount());
		}
	}
	
}
