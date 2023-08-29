package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 * 顺序扫描的access method，从一张表中读取每个元组（不依赖特定顺序）
 */
public class SeqScan implements OpIterator {
    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private String tableAlias;

    /**
     * 用于对元组进行迭代
     */
    private DbFileIterator dbFileIterator;

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 创建一个顺序扫描，在指定的table上，作为特定事务的一部分
     * @param tid        The transaction this scan is running as a part of.
     * @param tableId    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // TODO: some code goes here
        this.tid = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // TODO: some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableId    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableId, String tableAlias) {
        // TODO: some code goes here
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tid); // 这个对表tableId访问的迭代器属于某个事务
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     * 返回[来自底层HeapFile的字段名,以构造函数中的tableAlias为前缀]的TupleDesc
     * 当连接包含具有相同名称的字段的表时，该前缀变得有用
     * 别名和名称应该用"."字符分隔（例如，"alias.fieldName"）。
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        TupleDesc oldDesc = Database.getCatalog().getTupleDesc(tableId);
        Type[] typeAr = new Type[oldDesc.numFields()];
        String[] nameAr = new String[oldDesc.numFields()];
        for (int i = 0; i < oldDesc.numFields(); ++i) {
            typeAr[i] = oldDesc.getFieldType(i);
            nameAr[i] = tableAlias + "." + oldDesc.getFieldName(i); // 避免修改原引用的表元组字段名
        }
        return new TupleDesc(typeAr, nameAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (dbFileIterator == null) return false;
        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (dbFileIterator == null){
            throw new NoSuchElementException("The dbFileIterator is null");
        }
        Tuple t = dbFileIterator.next();
        if(t == null){
            throw new NoSuchElementException("The next tuple is null");
        }
        return t;
    }

    // 允许重复close!
    // TODO improve
    public void close() {
        // TODO: some code goes here
        if (dbFileIterator != null) {
            dbFileIterator.close();
            dbFileIterator = null;
        }
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        dbFileIterator.rewind();
    }
}
