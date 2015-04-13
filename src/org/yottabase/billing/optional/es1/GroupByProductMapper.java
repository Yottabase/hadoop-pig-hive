package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupByProductMapper extends
		Mapper<LongWritable, Text, Text, ProductCount> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",\t");
		
		Text product = new Text(tokenizer.nextToken());
		
		Text relatedProduct = new Text(tokenizer.nextToken());
		
		IntWritable count = new IntWritable( Integer.parseInt(tokenizer.nextToken()) );
		
		context.write(product, new ProductCount(relatedProduct, count));
	}
}