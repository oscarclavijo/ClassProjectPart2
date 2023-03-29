package CSCI485ClassProject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class DatabaseHelper {

    public static Database init() {

        FDB fdb = FDB.selectAPIVersion(710);
        Database db = null; 

        try {
            db = fdb.open();
        } catch (Exception e) {
            System.out.println(e);
        }
        return db; 

    } // end init() 

    /**
     * if a subdirectory exists db
     */
    public static boolean doesSubdirectoryExists(Transaction tx, List<String> path) {
        return DirectoryLayer.getDefault().exists(tx, path).join();
    }


} // end DatabaseHelper 