package org.vanilladb.core.storage.tx.concurrency;

import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class ConservativeConcurrencyMgr extends ConcurrencyMgr {

	public ConservativeConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}
	
	@Override
	public void onTxCommit(Transaction tx) {
		// Release all locks
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		// Release all locks
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// Do nothing
	}

	@Override
	public void modifyFile(String fileName) {
		// Do nothing
	}

	@Override
	public void readFile(String fileName) {
		// Do nothing
	}

	@Override
	public void insertBlock(BlockId blk) {
		// Do nothing
	}

	@Override
	public void modifyBlock(BlockId blk) {
		// Do nothing	
	}

	@Override
	public void readBlock(BlockId blk) {
		// Do nothing
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// Do nothing
	}

	@Override
	public void readRecord(RecordId recId) {
		// Do nothing
	}

	@Override
	public void modifyIndex(String dataFileName) {
		// Do nothing
	}

	@Override
	public void readIndex(String dataFileName) {
		// Do nothing
	}

	@Override
	public void modifyLeafBlock(BlockId blk) {
		// Do nothing
	}

	@Override
	public void readLeafBlock(BlockId blk) {
		// Do nothing
	}

	public void requestLocks(PrimaryKey[] readKeys, PrimaryKey[] writeKeys) {
		for(PrimaryKey key: readKeys) {
//			requestSLockKey(key, txNum);
    	}
    	for(PrimaryKey key: writeKeys) {
//    		requestXLockKey(key, txNum);
    	}		
	}
	
	public void getLocks(PrimaryKey[] readKeys, PrimaryKey[] writeKeys) {
    	for(PrimaryKey key: readKeys) {
    		// sLockKey(key, txNum);
			lockTbl.sLock(key, txNum);
    	}
    	for(PrimaryKey key: writeKeys) {
    		// xLockKey(key, txNum);
			lockTbl.xLock(key, txNum);
    	}
    }

	// public void sLockKey(PrimaryKey key, long txNum) {
	// 	String tableName = key.getTableName();
	// 	Map<String, Constant> keyEntryMap = key.getKeyEntryMap();
		
	// 	lockTbl.isLock(tableName, txNum);
	// 	for(String fld: keyEntryMap.keySet()) {
	// 		Constant keyVal = key.getKeyVal(fld);
	// 		lockTbl.sLock(keyVal, txNum);
	// 	}
	// }
	
	// public void xLockKey(PrimaryKey key, long txNum) {
	// 	String tableName = key.getTableName();
	// 	Map<String, Constant> keyEntryMap = key.getKeyEntryMap();

	// 	lockTbl.ixLock(tableName, txNum);
	// 	for(String fld: keyEntryMap.keySet()) {
	// 		Constant keyVal = key.getKeyVal(fld);
	// 		lockTbl.xLock(keyVal, txNum);
	// 	}
	// }
}
