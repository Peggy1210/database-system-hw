/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.benchmarks.as2.rte;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.util.BenchProperties;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.util.RandomValueGenerator;

public class As2BenchmarkRte extends RemoteTerminalEmulator<As2BenchTransactionType> { // TODO:

	// READ_WRITE_TX_RATE
	private static final double READ_WRITE_TX_RATE;
	
	private final int PRECISION = 100;
	
	static {
		READ_WRITE_TX_RATE = BenchProperties.getLoader()
				.getPropertyAsDouble(As2BenchmarkRte.class.getName() + ".READ_WRITE_TX_RATE", 1d);
	}

	public As2BenchmarkRte(SutConnection conn, StatisticMgr statMgr, long sleepTime) {
		super(conn, statMgr, sleepTime);
	}
	
	protected As2BenchTransactionType getNextTxType() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		if (rvg.number(0, PRECISION - 1) > (int) (READ_WRITE_TX_RATE * PRECISION))
			return As2BenchTransactionType.UPDATE_ITEM;
		else 
			return As2BenchTransactionType.READ_ITEM;
	} 
	 
	protected As2BenchmarkTxExecutor getTxExeutor(As2BenchTransactionType type) {
		if(type == As2BenchTransactionType.UPDATE_ITEM)
			return new As2BenchmarkTxExecutor(new UpdatePriceParamGen());
		else
			return new As2BenchmarkTxExecutor(new As2ReadItemParamGen());
	}
}

