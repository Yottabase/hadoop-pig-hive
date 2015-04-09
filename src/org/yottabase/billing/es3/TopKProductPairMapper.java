package org.yottabase.billing.es3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopKProductPairMapper extends
		Mapper<LongWritable, Text, ProductPair, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",\t");
		
		ProductPair pair = new ProductPair(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
		IntWritable count = new IntWritable(new Integer(tokenizer.nextToken()));
		
		context.write(pair, count);
		
	}

}
