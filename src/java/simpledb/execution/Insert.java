package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.IntField;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId _tid;
    private OpIterator _child;
    private int _tableId;
    private boolean _inserted;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // code done
        _tid = t;
        _child = child;
        _tableId = tableId;
        _inserted = false;
        // 两者的TupleDesc不相同时抛出异常
        if (!_child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
    }

    public TupleDesc getTupleDesc() {
        // code done
        // Insert的tuple desc就是一个Int Field，而不是子Op的tuple desc
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // code done
        super.open();
        _child.open();
    }

    public void close() {
        // code done
        super.close();
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // code done
        _child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // code done
        if (_child == null || _inserted) {
            return null;
        }
        int count = 0;
        while (_child.hasNext()) {
            Tuple tuple = _child.next();
            try {
                Database.getBufferPool().insertTuple(_tid, _tableId, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        _inserted = true;
        Tuple tuple = new Tuple(new TupleDesc(new Type[] { Type.INT_TYPE }));
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // code done
        return new OpIterator[] {_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        assert (children.length == 1);
        _child = children[0];
    }
}
