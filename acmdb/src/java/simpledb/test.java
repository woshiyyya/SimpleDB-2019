package simpledb;

import java.io.*;

public class test {

    public static void main(String[] argv) {

        // construct a 3-column table schema
//        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
//        String names[] = new String[]{ "field0", "field1", "field2" };
        int col = 1000;
        Type types[] = new Type[col];
        String names[] = new String[col];
        for (int i = 0; i < col; i++) {
            names[i] = "field" + String.valueOf(i);
            types[i] = Type.INT_TYPE;
        }
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("acmdb-lab1/some_data_1000*1000.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        }
    }

}