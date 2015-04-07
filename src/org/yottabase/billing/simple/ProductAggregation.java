package org.yottabase.billing.simple;

public class ProductAggregation {

	private String productName;
	
	private Integer count;
	
	public ProductAggregation(String productName, Integer count) {
		super();
		this.productName = productName;
		this.count = count;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}
	
	
}
