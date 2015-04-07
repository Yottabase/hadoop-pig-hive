package org.yottabase.billing.es1.onereducer;

import java.util.Comparator;

public class ProductAggregationComparator implements
		Comparator<ProductAggregation> {

	public int compare(ProductAggregation o1, ProductAggregation o2) {
		
		return (-1) * o1.getCount().compareTo(o2.getCount());
		
	}

}
