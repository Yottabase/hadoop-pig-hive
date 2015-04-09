package org.yottabase.billing.es3;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class TopKProductPairReducer extends
	Reducer<ProductPair, IntWritable, ProductPair, IntWritable> {
	
	private static final int K = 10;
	
	PriorityQueue<ProductPairCount> top = new PriorityQueue<ProductPairCount>(new ProductPairCountComparator());

	@Override
	public void reduce(ProductPair key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		for(IntWritable count : values){
			
			ProductPairCount ppc = new ProductPairCount(key, count);
			
			System.out.println(ppc);
			top.add( ppc );
			
			if(top.size() > K ){
				top.poll();
			}
		}
		
	}

	@Override
	protected void cleanup(
			Reducer<ProductPair, IntWritable, ProductPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//super.cleanup(context);
		
		for(ProductPairCount ppc : top){
			System.out.println(ppc);

			context.write(ppc.getPair(), ppc.getCount());
		}
	}
	
	
	
	
	
	
	/*
	public void reduce(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",\t");
		
		ProductPair pair = new ProductPair(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
		Integer count = new Integer(tokenizer.nextToken());
		
		top.add( new ProductPairCount(pair, count) );
		
		if(top.size() > K ){
			top.poll();
		}
	}


	@Override
	protected void cleanup(
			Reducer<LongWritable, Text, ProductPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		
	}*/
}
