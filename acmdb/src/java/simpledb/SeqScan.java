package simpledb;

import com.sun.org.apache.xpath.internal.axes.FilterExprIteratorSimple;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int table_id;
    private String table_alias;
    private DbFileIterator file_iterator; //iterate through all tuples in dbfile

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.table_id = tableid;
        this.table_alias = tableAlias;
        this.file_iterator = Database.getCatalog().getDatabaseFile(table_id).iterator(tid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return table_alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.table_id = tableid;
        this.table_alias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        file_iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(table_id);
        Type[] type_list = new Type[tupleDesc.numFields()];
        String[] field_list = new String[tupleDesc.numFields()];

        Iterator<TDItem> iter = tupleDesc.iterator();
        int ptr = 0;
        while (iter.hasNext()) {
            TDItem td = iter.next();
            type_list[ptr] = td.fieldType;
            if (table_alias != null) {
                field_list[ptr] = table_alias + "." + td.fieldName;
            } else {
                field_list[ptr] = td.fieldName;
            }
            ptr++;
        }
        return new TupleDesc(type_list, field_list);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return file_iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return file_iterator.next();
    }

    public void close() {
        file_iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        file_iterator.rewind();
    }
}
