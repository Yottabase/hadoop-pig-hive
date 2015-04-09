package org.yottabase.billing.es3;

import org.apache.hadoop.io.IntWritable;

public class ProductPairCount{
	
	private ProductPair pair;
	
	private IntWritable count;

	public ProductPairCount(ProductPair pair, IntWritable count) {
		super();
		this.pair = pair;
		this.count = count;
	}
	
	public ProductPair getPair() {
		return pair;
	}

	public void setPair(ProductPair pair) {
		this.pair = pair;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}	

	@Override
	public String toString() {
		return "ProductPairCount [pair=" + pair.toString() + ", count=" + count.toString() + "]";
	}
	
	
}
