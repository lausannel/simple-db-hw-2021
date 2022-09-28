package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId _tid;
    private OpIterator _child;
    private boolean _deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // code done
        _tid = t;
        _child = child;
    }

    public TupleDesc getTupleDesc() {
        // code done
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // code done
        if(_deleted || _child == null) {
            return null;
        }
        int count = 0;
        while (_child.hasNext()) {
            Tuple t = _child.next();
            try {
                Database.getBufferPool().deleteTuple(_tid, t);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        _deleted = true;
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
        // code done
        assert (children.length == 1);
        _child = children[0];
    }

}
