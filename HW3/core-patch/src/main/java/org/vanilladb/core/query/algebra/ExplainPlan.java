package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {
	// TODO:
	private Plan p;
	private Schema schema = new Schema();
	
	public ExplainPlan(Plan p) {
		this.p = p;
		schema.addField("query-plan", Type.VARCHAR(500));
	}
	
	@Override
	public Scan open() {
		return new ExplainScan(p.open(), schema(), outputString());
	}

	@Override
	public long blocksAccessed() {
		return p.blocksAccessed(); // TODO
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		return p.histogram(); // TODO
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput(); // TODO
	}

	@Override
	public String outputString() {
		return p.outputString();
	}
}
