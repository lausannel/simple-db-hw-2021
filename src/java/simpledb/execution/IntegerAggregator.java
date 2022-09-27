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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private HashMap<Field, Integer> _groupMap;
    private HashMap<Field, Integer> _countMap; // 只有在平均值的时候才用到

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // code done
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
        _groupMap = new HashMap<Field, Integer>(); // 要聚合的字段和对应的聚合值（最大、最小等）的映射
        if(_what == Op.AVG) {
            _countMap = new HashMap<Field, Integer>(); // 要聚合的字段的每个值对应的个数，只有在平均值的时候才用到
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = null;
        if (_gbfield != NO_GROUPING) {
            groupField = tup.getField(_gbfield);
        } else {
            groupField = new IntField(0); // 当没有group by的时候，groupField的值为0，为了方便后续从map中取值，当然，这里可以优化时间复杂度
        }
        int value = ((IntField) tup.getField(_afield)).getValue(); // 聚合对应的字段
        if (_groupMap.containsKey(groupField)) {
            int oldValue = _groupMap.get(groupField);
            if (_what == Op.MIN) {
                _groupMap.put(groupField, Math.min(oldValue, value));
            } else if (_what == Op.MAX) {
                _groupMap.put(groupField, Math.max(oldValue, value));
            } else if (_what == Op.SUM) {
                _groupMap.put(groupField, oldValue + value);
            } else if (_what == Op.AVG) {
                // if(_gbfield == NO_GROUPING) {
                //     // _groupMap.put(groupField, oldValue + value);
                //     System.out.println("avg, old value is " + oldValue);
                //     System.out.println("group Field is " + groupField);
                //     System.out.println("Count is " + _countMap.get(groupField));
                //     System.out.println("Put Value is " + (oldValue * _countMap.get(groupField) + value) / (_countMap.get(groupField) + 1));
                // }
                _groupMap.put(groupField, (oldValue  + value));
                _countMap.put(groupField, _countMap.get(groupField) + 1); // 更新计数
            } else if (_what == Op.COUNT) {
                _groupMap.put(groupField, oldValue + 1);
            }
        } else {
            if (_what == Op.MIN) {
                _groupMap.put(groupField, Integer.MAX_VALUE);
            } else if (_what == Op.MAX) {
                _groupMap.put(groupField, Integer.MIN_VALUE);
            } else if (_what == Op.SUM) {
                _groupMap.put(groupField, 0);
            } else if (_what == Op.AVG) {
                _groupMap.put(groupField, 0);
                _countMap.put(groupField, 0);
            } else if (_what == Op.COUNT) {
                _groupMap.put(groupField, 0);
            }
            mergeTupleIntoGroup(tup);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // code done
        // throw new
        // UnsupportedOperationException("please implement me for lab2");
        OpIterator it = new OpIterator() {
            // HashMap iterator
            private Iterator<Map.Entry<Field, Integer>> _it = null;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                _it = _groupMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return _it.hasNext();
            }

            @Override
            public void close() {
                _it = null;
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (_gbfield == NO_GROUPING) {
                    return new TupleDesc(new Type[] { Type.INT_TYPE }); // 如果不是聚合，那么返回一个值就可以
                } else {
                    return new TupleDesc(new Type[] { _gbfieldtype, Type.INT_TYPE }); // 如果是聚合，那么返回一个键值对，每个group字段的值对应一个聚合值
                }
            }
            
            // 最终聚合后返回的也是一个tuple
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Map.Entry<Field, Integer> entry = _it.next();
                Tuple tuple = new Tuple(getTupleDesc());
                if (_gbfield == NO_GROUPING) {
                    System.out.println("NO_GROUPING entry value is " + entry.getValue());
                    if (_what != Op.AVG) {
                        tuple.setField(0, new IntField(entry.getValue()));
                    } else {
                        tuple.setField(0, new IntField(entry.getValue() / _countMap.get(entry.getKey())));
                    }
                } else {
                    tuple.setField(0, entry.getKey());
                    if(_what != Op.AVG) {
                        tuple.setField(1, new IntField(entry.getValue()));
                    } else {
                        tuple.setField(1, new IntField(entry.getValue() / _countMap.get(entry.getKey())));
                    }                    
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                _it = _groupMap.entrySet().iterator(); // 重新回到Map的第一个元素
            }
        };
        return it;
    }

}
