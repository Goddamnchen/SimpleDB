package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator heapFileIt;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        heapFileIt = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        String tableName = Database.getCatalog().getTableName(tableId);
        if (tableName == null) return "null";
        else return tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        String alias = tableAlias;
        if (alias == null) return "null";
        else return alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.heapFileIt = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);

    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        heapFileIt.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = Database.getCatalog().getDatabaseFile(tableId).getTupleDesc();
        String[] fieldAr = new String[td.numFields()];
        Type[] typeAr = new Type[td.numFields()];
        // get Tuple descriptor for this table
        // read and modify each type and name in two new type&name array;
        // create and return new object of TupleDesc with new argument;
        Iterator<TupleDesc.TDItem> it = td.iterator();
        int index = 0;
        while (it.hasNext()) {
            TupleDesc.TDItem item = it.next();
            fieldAr[index] = getAlias() + "." + item.fieldName;
            typeAr[index] = item.fieldType;
            index++;
        }
        TupleDesc newTD = new TupleDesc(typeAr, fieldAr);
        return newTD;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return heapFileIt.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return heapFileIt.next();
    }

    public void close() {
        // some code goes here
        heapFileIt.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        heapFileIt.rewind();
    }
}
