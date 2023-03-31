// TODO: add stored procedure
package org.vanilladb.bench.server.procedure.as2;

import org.vanilladb.bench.server.param.as2.UpdatePriceProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class UpdatePriceProc extends StoredProcedure<UpdatePriceProcParamHelper> {

	public UpdatePriceProc() {
		super(new UpdatePriceProcParamHelper());
	}

	@Override
	protected void executeSql() {
		UpdatePriceProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
		
		// Update table
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			
			// Do SELECT
			Scan s = StoredProcedureHelper.executeQuery(
					"SELECT i_name, i_price FROM item WHERE i_id = " + iid,
					tx
				);
			s.beforeFirst();
			if (s.next()) {
				String name = (String) s.getVal("i_name").asJavaVal();
				double price = (Double) s.getVal("i_price").asJavaVal();

				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(price, idx);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);
			s.close();
			
			// Do UPDATE
			double updatedPrice = paramHelper.getItemUpdatedPrice(idx);
			int numUpdate = StoredProcedureHelper.executeUpdate(
					"UPDATE item SET i_price = " + updatedPrice + " WHERE i_id = " + iid,
					tx
				);
			if (numUpdate == 0)
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);

			
		}
	}
}
