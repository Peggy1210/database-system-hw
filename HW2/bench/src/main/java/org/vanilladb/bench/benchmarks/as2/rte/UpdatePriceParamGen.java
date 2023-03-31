// DONE: add parameter generator for UpdateItemPriceTxn
// reference: org.vanilladb.bench.benchmarks.as2.rte.As2ReadItemParamGen
package org.vanilladb.bench.benchmarks.as2.rte;

import java.util.ArrayList;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.BenchProperties;
import org.vanilladb.bench.util.RandomValueGenerator;

public class UpdatePriceParamGen implements TxParamGenerator<As2BenchTransactionType> {

	private final int MAX_PRICE_RAISE = 10;
	private final int PRECISION = 100;
	
	// Write Counts
	private static final int TOTAL_WRITE_COUNT;

	static {
		TOTAL_WRITE_COUNT = BenchProperties.getLoader()
				.getPropertyAsInteger(UpdatePriceParamGen.class.getName() + ".TOTAL_WRITE_COUNT", 10);
	}

	@Override
	public As2BenchTransactionType getTxnType() {
		return As2BenchTransactionType.UPDATE_ITEM;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();
		
		// Set write count
		paramList.add(TOTAL_WRITE_COUNT);
		for (int i = 0; i < TOTAL_WRITE_COUNT; i++) {
			paramList.add(rvg.number(1, As2BenchConstants.NUM_ITEMS));
			paramList.add((double) (rvg.number(0, MAX_PRICE_RAISE*PRECISION - 1) / PRECISION)); // generate random double value of price raise
		}
		return paramList.toArray(new Object[0]);
	}

}
