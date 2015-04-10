package org.yottabase.billing.optional.es1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductPair implements WritableComparable<ProductPair> {

	private Text firstProduct;

	private Text secondProduct;

	public ProductPair() {
		super();
	}

	public ProductPair(Text leftProduct, Text rightProduct) {
		super();
		this.changePair(leftProduct, rightProduct);
	}

	public Text getLeftProduct() {
		return firstProduct;
	}

	public Text getRightProduct() {
		return secondProduct;
	}

	public void changePair(Text leftProduct, Text rightProduct) {
		if (leftProduct.compareTo(rightProduct) <= 0) {
			this.firstProduct = leftProduct;
			this.secondProduct = rightProduct;
		} else {
			this.firstProduct = rightProduct;
			this.secondProduct = leftProduct;
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.firstProduct = new Text(in.readUTF());
		this.secondProduct = new Text(in.readUTF());

	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstProduct.toString());
		out.writeUTF(secondProduct.toString());
	}

	public int compareTo(ProductPair o) {

		int r = this.firstProduct.compareTo(o.getLeftProduct());

		if (r != 0) {
			return r;
		} else {
			return this.secondProduct.compareTo(o.getRightProduct());
		}

	}

	@Override
	public boolean equals(Object obj) {
		ProductPair that = (ProductPair) obj;

		return (this.firstProduct.equals(that.firstProduct) && this.secondProduct
				.equals(that.secondProduct));
	};

	@Override
	public String toString() {
		return this.firstProduct + "," + this.secondProduct;
	}

}
