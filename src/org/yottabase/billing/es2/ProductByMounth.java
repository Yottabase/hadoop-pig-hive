package org.yottabase.billing.es2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductByMounth implements WritableComparable<ProductByMounth> {

	private Text productName;

	private Integer mounth;

	public ProductByMounth() {
		super();
	}

	public ProductByMounth(Text productName, Integer mounth) {
		super();
		this.productName = productName;
		this.mounth = mounth;
	}

	public Text getProductName() {
		return productName;
	}

	public void setProductName(Text productName) {
		this.productName = productName;
	}

	public Integer getMounth() {
		return mounth;
	}

	public void setMounth(Integer mounth) {
		this.mounth = mounth;
	}

	public void readFields(DataInput in) throws IOException {
		this.productName = new Text(in.readUTF());
		this.mounth = new Integer(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(productName.toString());
		out.writeUTF(mounth.toString());
	}

	public int compareTo(ProductByMounth o) {

		int p = this.productName.compareTo(o.getProductName());
		int m = this.mounth.compareTo(o.getMounth());

		if (p != 0)
			return p;
		else if (m != 0)
			return m;
		else
			return 0;
	}

	@Override
	public String toString() {
		return productName + "\t" + mounth;
	}

}
