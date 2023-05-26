package org.vanilladb.core.storage.tx.concurrency;

import java.util.Map;

import org.vanilladb.core.sql.Constant;

// TODO
public class PrimaryKey {
  private String tableName;
  private Map<String, Constant> keyEntryMap;
  private int hashCode;

  public PrimaryKey(String tableName, Map<String, Constant> keyEntryMap) {
    this.tableName = tableName;
    this.keyEntryMap = keyEntryMap;
    generateHashCode();
  }

  public void generateHashCode() {
    hashCode = tableName.hashCode();
    for (String fld: keyEntryMap.keySet()) {
      hashCode += 31 * keyEntryMap.get(fld).hashCode();
    }
  }

  public String getTableName() {
    return tableName;
  }

  public Constant getKeyVal(String fld) {
    return keyEntryMap.get(fld);
  }
  
  public Map<String, Constant> getKeyEntryMap() {
    return keyEntryMap;
  }

  @Override
  public boolean equals(Object obj) {
    return hashCode() == obj.hashCode();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
