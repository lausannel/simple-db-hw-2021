package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import simpledb.common.Type;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Field[] fields;
    private TupleDesc td;
    private RecordId rid = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *           the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // code done
        // 创建一个新的tuple，其中包含一个新的field数组，其中每个field根据tupledesc创建
        this.td = td;
        fields = new Field[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            switch (td.getFieldType(i)) {
                case INT_TYPE:
                    fields[i] = new IntField(0);
                    break;
                case STRING_TYPE:
                    fields[i] = new StringField("", Type.STRING_LEN);
                    break;
                default:
                    throw new IllegalArgumentException("unknown type");
            }
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // code done
        // 返回tuple的tupleDesc
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // code done
        // 返回tuple的recordId
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // code done
        // 设置tuple的recordId
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *          index of the field to change. It must be a valid index.
     * @param f
     *          new value for the field.
     */
    public void setField(int i, Field f) {
        // code done
        // 将第i个field的值设置为f
        if (i < 0 || i >= td.numFields()) {
            throw new IllegalArgumentException("i is not a valid field reference");
        }
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *          field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // code done
        // 如果i是一个合法的field index，返回第i个field的值，否则抛出异常
        if (i < 0 || i >= td.numFields()) {
            throw new IllegalArgumentException("i is not a valid field reference");
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // code done
        // 将tuple的所有field的值拼接成一个字符串，用\t分隔
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < td.numFields(); i++) {
            sb.append(fields[i].toString());
            if (i != td.numFields() - 1) {
                sb.append("\t");
            }
        }
        return sb.toString();
        // throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *         An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // code done
        // 返回一个迭代器，迭代器返回tuple的所有field
        return Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // code done
        // 将tuple的tupleDesc重置为td
        this.td = td;
    }
}
