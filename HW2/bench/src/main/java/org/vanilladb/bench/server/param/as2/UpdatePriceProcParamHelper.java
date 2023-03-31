// DONE: resolve parameter on server side
// Reference: ReadItemProcParamHelper
package org.vanilladb.bench.server.param.as2;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureHelper;

public class UpdatePriceProcParamHelper implements StoredProcedureHelper {

	private int readCount;
	private int[] readItemId;
	private String[] itemName;
	private double[] itemPrice;
	private double[] itemPriceRaise;

	public int getReadCount() {
		return readCount;
	}

	public int getReadItemId(int index) {
		return readItemId[index];
	}

	/*
	 * Return a double value of price raise of the item with given index
	 */
	public double getItemPriceRaise(int index) {
		return itemPriceRaise[index];
	}
	
	/*
	 * Return a double value of updated price of the item with given index
	 */
	public double getItemUpdatedPrice(int index) {
		double updatedPrice = itemPrice[index] + itemPriceRaise[index];
		// If the price exceeds MAX_PRICE, adjust if to MIN_PRICE.
		// Otherwise, update the price to its original price + price raise
		if (itemPrice[index] > As2BenchConstants.MAX_PRICE)
			return (Double) As2BenchConstants.MIN_PRICE;
		else
			return updatedPrice;
	}

	public void setItemName(String s, int idx) {
		itemName[idx] = s;
	}

	public void setItemPrice(double d, int idx) {
		itemPrice[idx] = d;
	}

	@Override
	public void prepareParameters(Object... pars) {

		// Show the contents of parameters
		// System.out.println("Params: " + Arrays.toString(pars));

		int indexCnt = 0;

		readCount = (Integer) pars[indexCnt++];
		readItemId = new int[readCount];
		itemName = new String[readCount];
		itemPrice = new double[readCount];
		itemPriceRaise = new double[readCount];

		for (int i = 0; i < readCount; i++) {
			readItemId[i] = (Integer) pars[indexCnt++];
			itemPriceRaise[i] = (Double) pars[indexCnt++];
		}
	}

	@Override
	public Schema getResultSetSchema() {
		Schema sch = new Schema();
		Type intType = Type.INTEGER;
		Type itemPriceType = Type.DOUBLE;
		Type itemNameType = Type.VARCHAR(24);
		sch.addField("rc", intType);
		for (int i = 0; i < itemName.length; i++) {
			sch.addField("i_name_" + i, itemNameType);
			sch.addField("i_price_" + i, itemPriceType);
		}
		return sch;
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		SpResultRecord rec = new SpResultRecord();
		rec.setVal("rc", new IntegerConstant(itemName.length));
		for (int i = 0; i < itemName.length; i++) {
			rec.setVal("i_name_" + i, new VarcharConstant(itemName[i], Type.VARCHAR(24)));
			rec.setVal("i_price_" + i, new DoubleConstant(itemPrice[i]));
		}
		return rec;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}
}
