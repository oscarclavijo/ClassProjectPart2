package CSCI485ClassProject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.TableManager;
import CSCI485ClassProject.TableManagerImpl;
import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.models.Record;
public class Main {
    
    public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String[] EmployeeTableAttributeNames = new String[]{SSN, Name, Email, Age, Address};
  public static String[] EmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR};

  public static String[] UpdatedEmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address, Salary};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};

    public static int initialNumberOfRecords = 100;
  public static int updatedNumberOfRecords = initialNumberOfRecords / 2;

  private static String getName(long i) {
    return "Name" + i;
  }

  private static String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private static long getAge(long i) {
    return (i+25)%90;
  }

  private static String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private static long getSalary(long i) {
    return i + 100;
  }


    public static void main(String[] args) {
        TableManager a = new TableManagerImpl();
        a.dropAllTables();

        Records r = new RecordsImpl(); 

        a.dropAllTables();

        // create the Employee Table, verify that the table is created
        TableMetadata EmployeeTable = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes,
            EmployeeTablePKAttributes);
        a.createTable(EmployeeTableName,
            EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes);
        HashMap<String, TableMetadata> tables = a.listTables();
    
        for (int i = 0; i<initialNumberOfRecords; i++) {
          long ssn = i;
          String name = getName(i);
          String email = getEmail(i);
          long age = getAge(i);
          String address = getAddress(i);
    
          Object[] primaryKeyVal = new Object[] {ssn};
          Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address};
    
          r.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal);
        }
    
      
        Cursor cursor = r.openCursor(EmployeeTableName, Cursor.Mode.READ);
    
        // initialize the first record
        Record rec = r.getFirst(cursor);
        // verify the first record

        for (int i = initialNumberOfRecords; i<initialNumberOfRecords + updatedNumberOfRecords; i++) {
            long ssn = i;
            String name = getName(i);
            String email = getEmail(i);
            long age = getAge(i);
            String address = getAddress(i);
            long salary = getSalary(i);
      
      
            Object[] primaryKeyVal = new Object[] {ssn};
            Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address, salary};
      
            r.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, UpdatedEmployeeTableNonPKAttributeNames, nonPrimaryKeyVal);
          }
      
          // verify the schema changing
          TableMetadata expectedEmployeeTableSchema = new TableMetadata();
          expectedEmployeeTableSchema.addAttribute(SSN, AttributeType.INT);
          expectedEmployeeTableSchema.addAttribute(Name, AttributeType.VARCHAR);
          expectedEmployeeTableSchema.addAttribute(Email, AttributeType.VARCHAR);
          expectedEmployeeTableSchema.addAttribute(Address, AttributeType.VARCHAR);
          expectedEmployeeTableSchema.addAttribute(Age, AttributeType.INT);
          expectedEmployeeTableSchema.addAttribute(Salary, AttributeType.INT);
          expectedEmployeeTableSchema.setPrimaryKeys(Collections.singletonList("SSN"));
      
          HashMap<String, TableMetadata> ts = a.listTables();
          System.out.println(ts.get(EmployeeTableName));
          for (Map.Entry<String,AttributeType> x : ts.get(EmployeeTableName).getAttributes().entrySet()) {

            System.out.println(x.getKey());
          }
    
        // Cursor c = r.openCursor(EmployeeTableName, null);


        // Scanner input = new Scanner(System.in); 
        // String op = "";
        // do {
        //     System.out.println("Next op: "); 
        //     op = input.nextLine();
        //     if (op.contains("n")) {
        //         // next 
        //         r.getNext(c);
        //     } else if (op.contains("p")) {
        //         // prev
        //     } else if (op.contains("f")) {
        //         r.getFirst(c);
        //     }
        //     op = "";
        // } while (!op.contains("q"));
        // input.close();
        
        // System.out.println(records.insertRecord(EmployeeTableName, new String[]{}, new String[]{}, new String[]{"Name"}, new Object[]{"Bob"}));
        // System.out.println(records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{initialNumberOfRecords+1}, new String[]{"Name"}, new Object[]{12345}));
    
    }
}
