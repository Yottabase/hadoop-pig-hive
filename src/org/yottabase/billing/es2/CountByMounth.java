package org.yottabase.billing.es2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CountByMounth implements WritableComparable<CountByMounth> {

	private Integer mounth;

	private Integer count;

	public CountByMounth(Integer mounth, Integer count) {
		super();
		this.mounth = mounth;
		this.count = count;
	}

	public CountByMounth() {
		super();
	}

	public Integer getMounth() {
		return mounth;
	}

	public void setMounth(Integer mounth) {
		this.mounth = mounth;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
		this.mounth = new Integer(in.readUTF());
		this.count = new Integer(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(mounth.toString());
		out.writeUTF(count.toString());
	}

	public int compareTo(CountByMounth o) {
		return this.mounth.compareTo(o.getMounth());
	}

	@Override
	public String toString() {
		return (this.mounth + 1) + "/2015:" + this.count;
	}
}
