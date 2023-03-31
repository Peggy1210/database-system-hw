// DOING: add JDBC of UpdateItemPriceTxn
package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.bench.server.param.as2.UpdatePriceProcParamHelper;

public class UpdatePriceJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdatePriceJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		UpdatePriceProcParamHelper paramHelper = new UpdatePriceProcParamHelper();
		paramHelper.prepareParameters(pars);
		
		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
		
		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			int numUpdate = 0;
			
			// Update table
			for (int i = 0; i < paramHelper.getReadCount(); i++) {
				int iid = paramHelper.getReadItemId(i);
				
				// Do SELECT
				String sql_select = "SELECT i_name, i_price FROM item WHERE i_id = " + iid;
				rs = statement.executeQuery(sql_select);
				rs.beforeFirst();
				if (rs.next()) {
					outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
					
					// Set
					String name = (String) rs.getString("i_name");
					double price = (Double) rs.getDouble("i_price");
					paramHelper.setItemName(name, i);
					paramHelper.setItemPrice(price, i);
				} else
					throw new RuntimeException("cannot find the record with i_id = " + iid);
				rs.close();
				
				// Do UPDATE
				double updatedPrice = paramHelper.getItemUpdatedPrice(i);
				String sql_update = "UPDATE item SET i_price = " + updatedPrice + " WHERE i_id = " + iid;
                numUpdate = statement.executeUpdate(sql_update);
                if (numUpdate == 0)
                	throw new RuntimeException("cannot find the record with i_id = " + iid);                	
			}
			
			conn.commit();

			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");
			
			return new VanillaDbJdbcResultSet(true, "");
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
	
}
