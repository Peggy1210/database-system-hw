package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	private Scan s;
	private Schema schema;
	private String explain;
	private int numRecs;
	private boolean isBeforeFirst = true;
	
	public ExplainScan(Scan s, Schema schema, String explain) {
		this.s = s;
		this.schema = schema;
		this.numRecs = 0;
		
		s.beforeFirst();
		while(s.next())
			this.numRecs++;
		s.close();
		
		this.explain = "\n" + explain + "\nActual #recs: " + numRecs;
	}
	
	@Override
	public Constant getVal(String fldName) {
		if (fldName.contentEquals("query-plan")) {
			return new VarcharConstant(explain);
		} else {
			throw new RuntimeException("filed " + fldName + " not found.");
		}
	}

	@Override
	public void beforeFirst() {
//		s.beforeFirst(); // TODO
		this.isBeforeFirst = true;
	}

	@Override
	public boolean next() {
//		while (s.next())
//			return true; // TODO 
//		return false;
		if (isBeforeFirst) {
			isBeforeFirst = false;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void close() {
		s.close(); // TODO 
	}

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName); // TODO 
	}

}
