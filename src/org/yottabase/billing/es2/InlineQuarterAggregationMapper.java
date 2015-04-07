package org.yottabase.billing.es2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InlineQuarterAggregationMapper extends
		Mapper<LongWritable, Text, Text, CountByMounth> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		String product = tokenizer.nextToken();
		Integer mounth = new Integer(tokenizer.nextToken());
		Integer count = new Integer(tokenizer.nextToken());
		
		context.write(new Text(product), new CountByMounth(mounth, count));
		
	}

}