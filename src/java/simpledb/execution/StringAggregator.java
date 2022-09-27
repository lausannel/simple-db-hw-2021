package simpledb.execution;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import java.util.*;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private HashMap<Field, Integer> _groupMap = null;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException{
        // code done
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        if(what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        _what = what;
        _groupMap = new HashMap<Field, Integer>(); // 要聚合的字段和对应的聚合值（字符串类型只支持COUNT）的映射
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // System.out.println("StringAggregator.mergeTupleIntoGroup");
        Field groupField = null;
        if (_gbfield != NO_GROUPING) {
            groupField = tup.getField(_gbfield);
        }
        if (_groupMap.containsKey(groupField)) {
            _groupMap.put(groupField, _groupMap.get(groupField) + 1); // count + 1
            // System.out.println("groupField: " + groupField + " count: " + _groupMap.get(groupField));
        } else {
            _groupMap.put(groupField, 0);
            mergeTupleIntoGroup(tup);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // code done
        // throw new UnsupportedOperationException("please implement me for lab2");
        OpIterator it = new OpIterator() {
            // map iterator
            Iterator<Map.Entry<Field, Integer>> _it = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                _it = _groupMap.entrySet().iterator();
            }

            @Override
            public void close() {
                _it = null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                _it = _groupMap.entrySet().iterator();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (_it == null) {
                    throw new NoSuchElementException();
                }
                Map.Entry<Field, Integer> entry = _it.next();
                Tuple tuple = new Tuple(getTupleDesc());
                if (_gbfield == NO_GROUPING) {
                    tuple.setField(0, new IntField(entry.getValue()));
                } else {
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                }
                // System.out.println("StringAggregator.iterator.next: " + tuple);
                return tuple;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return _it != null && _it.hasNext();
            }

            @Override
            public TupleDesc getTupleDesc() {
                TupleDesc td = null;
                if (_gbfield == NO_GROUPING) {
                    td = new TupleDesc(new Type[]{Type.INT_TYPE});
                } else {
                    td = new TupleDesc(new Type[] {_gbfieldtype, Type.INT_TYPE });
                }
                return td;
            }

        };
        return it;
    }

}
