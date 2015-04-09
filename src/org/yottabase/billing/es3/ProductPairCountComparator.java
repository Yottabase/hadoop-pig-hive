package org.yottabase.billing.es3;

import java.util.Comparator;

public class ProductPairCountComparator implements
		Comparator<ProductPairCount> {

	public int compare(ProductPairCount o1, ProductPairCount o2) {
		
		return o1.getCount().get() - o2.getCount().get();
		
	}

}
