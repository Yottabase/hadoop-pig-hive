package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.yottabase.billing.optional.es1.ProductPair;

public class ProductPairMapper extends
		Mapper<LongWritable, Text, ProductPair, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		List<Text> products = new ArrayList<Text>();

		List<ProductPair> pairLine = new ArrayList<ProductPair>();

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		tokenizer.nextToken(); // salta data

		while (tokenizer.hasMoreTokens()) {
			products.add(new Text(tokenizer.nextToken()));
		}

		Text left, right;

		for (int i = 0; i < products.size() - 1; i++) {

			left = products.get(i);

			for (int j = i + 1; j < products.size(); j++) {

				right = products.get(j);

				ProductPair pp = new ProductPair(left, right);

				if (!pairLine.contains(pp)) {
					pairLine.add(pp);
					context.write(pp, ONE);
				}

			}
		}

	}
}