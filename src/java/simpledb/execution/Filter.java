package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate _predicate;
    private OpIterator _child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // code done
        _predicate = p;
        _child = child;
        
    }

    public Predicate getPredicate() {
        // code done
        return _predicate;
    }

    public TupleDesc getTupleDesc() {
        // code done
        return _child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // code done
        _child.open();
        super.open();
    }

    public void close() {
        // code done
        _child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // code done
        _child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // code done
        Tuple tuple = null;
        while (_child.hasNext()) { // 用_child operator去找下一个tuple，然后判断是否符合要求
            tuple = _child.next();
            
            if (_predicate.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // code done
        return new OpIterator[] {_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // code done 
        _child = children[0];
    }

}
