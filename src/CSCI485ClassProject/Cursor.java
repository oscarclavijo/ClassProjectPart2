package CSCI485ClassProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.ComparisonOperator;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.ReadTransaction; 

public class Cursor implements AsyncIterator<Record> {

  private Database db; 
  private AsyncIterator<KeyValue> iterator;
  private DirectorySubspace dir; 
  private Mode selectedMode; 
  private Transaction t;
  private TableMetadata tbl; 
  private boolean forward; 
  private boolean cancel; 

  private boolean predicate; 
  private ComparisonOperator operator; 
  private String attrName; 
  private Object attrValue; 

  
  public enum Mode {
    READ,
    READ_WRITE
  }

  public Cursor(String tableName, Database db, String attrName, Object attrValue, ComparisonOperator operator, Mode mode, boolean isUsingIndex) {
    this.db = db; 
    selectedMode = mode; 
    this.attrName = attrName;
    this.attrValue = attrValue;
    this.operator = operator;
    predicate = true; 
    
    t = db.createTransaction(); 
    List<String> path = new ArrayList<>();
    path.add(tableName);
    path.add("dataRecords");

    dir = DirectoryLayer.getDefault().createOrOpen(t, path).join(); 
    tbl = RecordsImpl.getTableByName(tableName, t);
    
    cancel = false;
    
  }

  public Cursor(String tableName, Database db, Mode m) {
    this.db = db; 
    selectedMode = m; 
    
    t = db.createTransaction(); 
    List<String> path = new ArrayList<>();
    path.add(tableName);
    path.add("dataRecords");

    dir = DirectoryLayer.getDefault().createOrOpen(t, path).join(); 
    tbl = RecordsImpl.getTableByName(tableName, t);
    
    cancel = false;
  }

  public Record getFirst() {
    iterator = t.getRange(dir.range()).iterator(); 
    forward = true;
    return getNext();
  }

  public Record getLast() { 
    iterator = t.getRange(dir.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, true).iterator();
    forward = false; 
    return getPrevious(); 
  }
  @Override
  public CompletableFuture<Boolean> onHasNext() {
    if (cancel) 
      return CompletableFuture.completedFuture(false);
    
    return iterator.onHasNext();
  }

  @Override
  public boolean hasNext() {
    return false; 
  }

  public Record getPrevious() { 
    if (forward) return null; 
    if (predicate) return nextWithPredicate();
    return next();
  }

  public Record getNext() { 
    if (!forward) return null; 
    if (predicate) return nextWithPredicate();
    return next();
  }

  @Override
  public Record next() {
    if(!iterator.hasNext()) {
      return null; 
    }

    KeyValue curr = iterator.next();
    long key = dir.unpack(curr.getKey()).getLong(0);
    HashMap<String, Object> row = getRow(t, dir, key);
    for (int i = 1; i < row.size(); i++) iterator.next(); 

    Record r = generateRecordFromRow(key, row); 
    return r; 
  }

  public Record nextWithPredicate() { 
    boolean skip = true; 
    Record r; 
    do {
      r = next(); 
      skip = includeThisRecord(r);
      
    } while(skip); 
    return r; 
  }
  
  @Override
  public void cancel() {
    TableManagerImpl.commit(t);
    cancel = true; 
  }

  private boolean includeThisRecord(Record r) {
    boolean include = true; 
    AttributeType storedType = r.getTypeForGivenAttrName(attrName); 
    Object storedValue = r.getValueForGivenAttrName(attrName);
    int comparisonValue; 

    if (storedType == AttributeType.VARCHAR) {
      comparisonValue = storedValue.toString().compareTo(attrValue.toString()); 
    } else if (storedType == AttributeType.NULL) {
      comparisonValue = 0;
    } else {
      comparisonValue = (int)attrValue - (int)storedValue;
    }

    if(operator == ComparisonOperator.EQUAL_TO) {
      include = comparisonValue == 0; 
    } else if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      include = comparisonValue >= 0; 
    } else if (operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
      include = comparisonValue <= 0; 
    } else if (operator == ComparisonOperator.GREATER_THAN) {
      include = comparisonValue > 0; 
    } else if (operator == ComparisonOperator.LESS_THAN) {
      include = comparisonValue < 0; 
    }  
    return include; 
  }

  private Record generateRecordFromRow(long key, HashMap<String, Object> row) {
    Record r = new Record(); 
    
    r.setMapAttrNameToValue(row);

    Map<String, AttributeType> attributes = tbl.getAttributes(); 
    // all cells have a value. no further processing. 
    if (tbl.getAttributes().size() == row.size()) {
      return r; 
    } else { 
      for (Map.Entry<String, AttributeType> a : attributes.entrySet()) {
        if (!row.containsKey(a.getKey())) {
          r.setAttrNameAndValue(a.getKey(), null);
        }
      } // end for each
    } // end else

    List<String> pkName = tbl.getPrimaryKeys(); 
    r.setAttrNameAndValue(pkName.get(0), key);

    return r; 
  } // end generateRecordFromRow

  private HashMap<String, Object> getRow(Transaction tr, DirectorySubspace tableSpace, long row) {
    HashMap<String, Object> cols = new HashMap<>();

    byte[] beginKey = tableSpace.pack(Tuple.from(row));
    byte[] endKey = tableSpace.pack(Tuple.from(row + 1)); 

    for (KeyValue keyValue : tr.getRange(beginKey, endKey).asList().join()) {
        Tuple keyTuple = tableSpace.unpack(keyValue.getKey());
        String column = keyTuple.getString(1);
        byte[] value = keyValue.getValue();

        Object o = Tuple.fromBytes(value).get(0);
        cols.put(column, o);
    }

    return cols;
  } // end getrow

  
}
