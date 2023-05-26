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
package org.vanilladb.bench.server.procedure.micro;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConservativeConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.PrimaryKey;

public class MicroTxnProc extends StoredProcedure<MicroTxnProcParamHelper> {

	public MicroTxnProc() {
		super(new MicroTxnProcParamHelper());
	}
	
	private static Object globalLock = new Object();

	@Override
	protected void executeSql() { // TODO
		MicroTxnProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
//		Transaction tx;
//		
//		int isolationLevel = getIsolationLevel();
//		if (isolationLevel == 16) {
//			// TODO
//			PrimaryKey[] readKeys = getReadKeys(paramHelper);
//			PrimaryKey[] writeKeys = getWriteKeys(paramHelper);
////			tx = getTransaction();
//			tx = prepareTx(readKeys, writeKeys);
////			prepareKeys(readKeys, writeKeys);
////			ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
////			ccMgr.getLocks(readKeys, writeKeys, "i_id");
//		}
//		else {
//			tx = getTransaction();
//		}
		
		// SELECT
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
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
		}
		
		// UPDATE
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			StoredProcedureHelper.executeUpdate(
				"UPDATE item SET i_price = " + newPrice + " WHERE i_id =" + iid,
				tx
			);
		}
	}
	
	// TODO
	private PrimaryKey[] getReadKeys(MicroTxnProcParamHelper paramHelper) {
		PrimaryKey[] readKeys = new PrimaryKey[paramHelper.getReadCount()];
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(iid));
			PrimaryKey key = new PrimaryKey("item", keyEntryMap);
			
			readKeys[idx] = key;
		}
		return readKeys;
	}
	
	// TODO
	private PrimaryKey[] getWriteKeys(MicroTxnProcParamHelper paramHelper) {
		PrimaryKey[] writeKeys = new PrimaryKey[paramHelper.getWriteCount()];
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(iid));
			PrimaryKey key = new PrimaryKey("item", keyEntryMap);
			
			writeKeys[idx] = key;
		}
		return writeKeys;
	}
	
	@Override
	public void prepare(Object... pars) {
		// prepare parameters
		MicroTxnProcParamHelper paramHelper = getParamHelper();
		paramHelper.prepareParameters(pars);
		
		int isolationLevel = getIsolationLevel();
		Transaction tx;
		
		// create a transaction
		if(isolationLevel == 16) {
			PrimaryKey[] readKeys = getReadKeys(paramHelper);
			PrimaryKey[] writeKeys = getWriteKeys(paramHelper);
			tx = prepareTx(readKeys, writeKeys);

			// Modified Version:
			// ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
			// ccMgr.getLocks(readKeys, writeKeys);
		} else {
			boolean isReadOnly = paramHelper.isReadOnly();
			tx = VanillaDb.txMgr().newTransaction(
					isolationLevel, isReadOnly);
		}
		
		setTx(tx);
	}

	public Transaction prepareTx(PrimaryKey[] readKeys, PrimaryKey[] writeKeys) {
		MicroTxnProcParamHelper paramHelper = getParamHelper();
		int isolationLevel = getIsolationLevel();
		boolean isReadOnly = paramHelper.isReadOnly();
		Transaction tx;
		synchronized (globalLock) {
			tx = VanillaDb.txMgr().newTransaction(isolationLevel, isReadOnly);
			ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
			ccMgr.getLocks(readKeys, writeKeys);
			
			// Modified Version:
			// ccMgr.requestLocks(readKeys, writeKeys);
		}
		return tx;
	}
}
