package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId _tid;
    private int _tableid;
    private String _tableAlias;
    private DbFileIterator _iterator;

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
        // code done
        _tid = tid;
        _tableid = tableid;
        _tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(_tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // code done
        return _tableAlias;
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
        // code done
        _tableid = tableid;
        _tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // code done
        _iterator = Database.getCatalog().getDatabaseFile(_tableid).iterator(_tid); // 找到对应的文件的iterator
        _iterator.open();  // 打开文件的iterator相当于打开这个iterator
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
        TupleDesc td = Database.getCatalog().getTupleDesc(_tableid);
        int numFields = td.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];
        // 一个个地手动拷贝过来，因为type和field都是private的
        for (int i = 0; i < numFields; i++) {
            typeAr[i] = td.getFieldType(i);
            fieldAr[i] = _tableAlias + "." + td.getFieldName(i);
        }
        return new TupleDesc(typeAr, fieldAr); // 返回一个新的TupleDesc，Type是原来Field的Type，Name加上了tableAlias的前缀
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // code done
        if (_iterator == null) { // 说明还没有open
            return false;
        }
        return _iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // code done
        if (_iterator == null) { // null情况下不能调用next
            throw new NoSuchElementException();
        }
        return _iterator.next(); // 底层已经封装好了，直接调用就可以了
    }

    public void close() {
        // code done
        _iterator.close();
        _iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // code done
        _iterator.rewind();
    }
}
