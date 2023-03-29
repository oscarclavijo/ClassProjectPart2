package CSCI485ClassProject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import java.io.ObjectInputFilter.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import CSCI485ClassProject.models.*;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.subspace.Subspace;
import java.util.ArrayList;
import java.util.List;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.Record.Value;

public class RecordsImpl implements Records{

  private Database db = null;
  private static int MAX_TRANSACTION_COMMIT_RETRY_TIMES = 20;

  /**
   * Init database. 
   */
  public RecordsImpl() {
    FDB fdb = FDB.selectAPIVersion(710);
    try {
        db = fdb.open();
    } catch (Exception e) {
        System.out.println(e);
    }
  }

  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {

    Transaction t = db.createTransaction();

    TableMetadata insertTo = getTableByName(tableName, t);
    if (insertTo == null) { 
      t.cancel();
      return StatusCode.TABLE_NOT_FOUND;
    }

    List<String> tablePK = insertTo.getPrimaryKeys();
    HashMap<String, AttributeType> attributesInCurr = insertTo.getAttributes(); 

    // error check: if there is a pk unmatched
    if (!tablePK.equals(Arrays.asList(primaryKeys))) {
      t.cancel();
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED; 
    }

    /**
     * Data Model: keyTuple is (PK, attrName) = attrValue. 
     * Verify that data types match by using Record functions and comparing 
     * to TableMetadata. 
     */
    List<String> dataPath = getDataPathForTable(tableName);
    DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, dataPath).join();
  
    Record r = new Record(); 
    Tuple keyTuple = new Tuple(); 
    Tuple valueTuple = new Tuple(); 

    for (int i = 0; i < primaryKeysValues.length; i++) {
      r.setAttrNameAndValue(primaryKeys[i], primaryKeysValues[i]);
      
      for (int j = 0; j < attrValues.length; j++) {
        if (r.setAttrNameAndValue(attrNames[j], attrValues[j]) == StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED) {
          t.cancel();
          return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
        }

        if(!attributesInCurr.containsKey(attrNames[j])) {
          addAttributeToTable(tableName, r.getTypeForGivenAttrName(attrNames[j]), attrNames[j]);
          insertTo = getTableByName(tableName, t);
          attributesInCurr = insertTo.getAttributes(); 
        }

        if(attributesInCurr.get(attrNames[j]) != r.getTypeForGivenAttrName(attrNames[j])) {
          t.cancel();
          return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
        }

        keyTuple = Tuple.from(r.getValueForGivenAttrName(primaryKeys[i])).add(attrNames[j]);
        valueTuple = Tuple.from(r.getValueForGivenAttrName(attrNames[j])); 

        t.set(dir.pack(keyTuple), valueTuple.pack());
      } // end for loop(attrValues)
    } // end for loop (primaryKeysValues) 

    TableManagerImpl.commit(t);
    return StatusCode.SUCCESS;

  } // end insert

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    Cursor c = new Cursor(tableName, db, attrName, attrValue, operator, mode, isUsingIndex);
    return c; 
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    Cursor c = new Cursor(tableName, db, Cursor.Mode.READ);
    return c;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    return cursor.getFirst();
  }

  @Override
  public Record getLast(Cursor cursor) {
    return cursor.getLast();
  }

  @Override
  public Record getNext(Cursor cursor) {
    return cursor.getNext(); 
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return cursor.getPrevious();
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return null;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    cursor.cancel();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }

  public static List<String> getDataPathForTable(String tableName) {
    List<String> path = new ArrayList<>(); 
    path.add(tableName);
    path.add("dataRecords");
    return path; 
  }

  public static List<String> getAttributePathForTable(String tableName) {
    List<String> path = new ArrayList<>(); 
    path.add(tableName);
    path.add("attributesStored");
    return path; 
  }

  public void addAttributeToTable(String tableName, AttributeType type, String name) {
    Transaction t = db.createTransaction(); 
    List<String> dataPath = getAttributePathForTable(tableName);
    DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, dataPath).join();

    Tuple keyTuple = new Tuple().add(name); 
    Tuple valueTuple = new Tuple().add(type.ordinal()).add(false); 
    t.set(dir.pack(keyTuple), valueTuple.pack());
    t.commit(); 

  }

  public static TableMetadata getTableByName(String tableName, Transaction t) { 
    List<String> names = DirectoryLayer.getDefault().list(t).join();

    for (String tblName : names) {
      // equal tableNames 
      if (tblName.compareTo(tableName) == 0) {

        DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, getAttributePathForTable(tableName)).join();
        Range range = dir.range();

        List<KeyValue> kvs = t.getRange(range).asList().join();

        TableMetadata tableMetadata = new TableMetadata();
        List<String> primaryKeys = new ArrayList<>();

        for (KeyValue kv : kvs) {
          Tuple key = dir.unpack(kv.getKey());
          Tuple value = Tuple.fromBytes(kv.getValue());
  
          String attributeName = key.getString(0);
          tableMetadata.addAttribute(attributeName, AttributeType.values() [Math.toIntExact((Long) value.get(0))]);
          boolean isPrimaryKey = value.getBoolean(1);
          if (isPrimaryKey) {
            primaryKeys.add(attributeName);
          }
        }
        tableMetadata.setPrimaryKeys(primaryKeys);
        return tableMetadata; 
      }
    }// end for loop 

    return null; 
  }

}
