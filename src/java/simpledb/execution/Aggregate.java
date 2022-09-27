package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator _child; // Iterator
    private int _afield; // aggregate field
    private int _gbfield; // group by field
    private Aggregator.Op _aop; // aggregate type
    private Aggregator _aggregator; // aggregator
    private OpIterator _iterator = null; // iterator

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // code done
        _child = child;
        _afield = afield;
        _gbfield = gfield;
        _aop = aop;
        TupleDesc td = _child.getTupleDesc();
        Type gbfieldtype = null;
        if(_gbfield != Aggregator.NO_GROUPING) {
            gbfieldtype = td.getFieldType(_gbfield);
        }
        if (td.getFieldType(_afield) == Type.INT_TYPE) {
            _aggregator = new IntegerAggregator(_gbfield, gbfieldtype, afield, _aop);
        } else if (td.getFieldType(_afield) == Type.STRING_TYPE) {
            _aggregator = new StringAggregator(_gbfield, gbfieldtype, afield, _aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // code done
        if (_aop != null) {
            return _gbfield;
        }
        return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // code done
        if (_aop != null) {
            return _child.getTupleDesc().getFieldName(_gbfield);
        }
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // code done
        return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // code done
        if (_aop != null) {
            return _child.getTupleDesc().getFieldName(_afield);
        }
        return null;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // code done
        return _aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // code done
        super.open();
        _child.open();
        // Opemn过一次之后所有的tuple都在aggregator里面了，接下来只需要每次都在aggregator里面操作就行了
        while (_child.hasNext()) {
            _aggregator.mergeTupleIntoGroup(_child.next());
        }
        _iterator = _aggregator.iterator();
        _iterator.open();
        _child.close();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // code done
        // System.out.println("fetchNext");
        // while (_iterator.hasNext()) {
        //     System.out.println(_iterator.next());
        // }
        if (_iterator.hasNext()) {
            return _iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // code done
        _iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // code done
        TupleDesc td = null;
        if (_gbfield == Aggregator.NO_GROUPING) {
            // td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] {
            // "aggName(" + _aop.toString() + ") (" + _child.getTupleDesc().getFieldName(_afield) + ")" });
            td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggName(" + _aop.toString() + ") ()" });
        } else {
            // td = new TupleDesc(new Type[] { _child.getTupleDesc().getFieldType(_gbfield), Type.INT_TYPE },
            //         new String[] {
            //                 _child.getTupleDesc().getFieldName(_gbfield),
            //                 "aggName(" + _aop.toString() + ") (" + _child.getTupleDesc().getFieldName(_afield) + ")" });
            td = new TupleDesc(new Type[] {_child.getTupleDesc().getFieldType(_gbfield), Type.INT_TYPE}, 
                        new String[] {
                            "",
                            "aggName(" + _aop.toString() + ") ()"
                        });
        }
        // System.out.println(td);
        return td;
    }

    public void close() {
        // code done
        super.close();
        _iterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // code done
        assert (_child != null);
        return new OpIterator[]{_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // code done
        _child = children[0];
    }

}
