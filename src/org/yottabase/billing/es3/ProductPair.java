package org.yottabase.billing.es3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductPair implements WritableComparable<ProductPair> {

	private Text leftProduct;

	private Text rightProduct;

	public ProductPair() {
		super();
	}

	public ProductPair(Text leftProduct, Text rightProduct) {
		super();
		this.changePair(leftProduct, rightProduct);
	}

	public Text getLeftProduct() {
		return leftProduct;
	}

	public Text getRightProduct() {
		return rightProduct;
	}

	public void changePair(Text leftProduct, Text rightProduct) {
		if (leftProduct.compareTo(rightProduct) <= 0) {
			this.leftProduct = leftProduct;
			this.rightProduct = rightProduct;
		} else {
			this.leftProduct = rightProduct;
			this.rightProduct = leftProduct;
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.leftProduct = new Text(in.readUTF());
		this.rightProduct = new Text(in.readUTF());

	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(leftProduct.toString());
		out.writeUTF(rightProduct.toString());
	}

	public int compareTo(ProductPair o) {

		int r = this.leftProduct.compareTo(o.getLeftProduct());

		if (r != 0) {
			return r;
		} else {
			return this.rightProduct.compareTo(o.getRightProduct());
		}

	}

	@Override
	public String toString() {
		return  this.leftProduct + "," + this.rightProduct;
	}
	
	

}
