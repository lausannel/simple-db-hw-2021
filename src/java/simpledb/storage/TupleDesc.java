package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final TDItem[] items;

    /**
     * @return
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // code done
        // 返回开头的iterator
        Iterator<TDItem> it = Arrays.asList(items).iterator();
        return it;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // code done
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("typeAr and fieldAr must have the same length");
        }
        for (int i = 0; i < typeAr.length; i++) {
            if (typeAr[i] == null) {
                throw new IllegalArgumentException("typeAr must not contain null");
            }
        }
        for (int i = 0; i < fieldAr.length; i++) {
            if (fieldAr[i] == null) {
                throw new IllegalArgumentException("fieldAr must not contain null");
            }
        }
        this.items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // code done
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must not be empty");
        }
        for (int i = 0; i < typeAr.length; i++) {
            if (typeAr[i] == null) {
                throw new IllegalArgumentException("typeAr must not contain null");
            }
        }
        this.items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            this.items[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // code done
        return items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // code done
        // 判断i是否合法，返回第i个item的fieldName
        if (i < 0 || i >= items.length) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // code done
        if (i < 0 || i > items.length) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // code done
        // if not found throw exception
        if (name == null) {
            throw new NoSuchElementException("name is null");
        }
        for (int i = 0; i < items.length; i++) {
            // System.out.println(items[i].fieldName);
            if (items[i].fieldName == null) {
                throw new NoSuchElementException("name is null");
            }
            if (items[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field with the given name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // code done
        // todo，size参数看看是否可以缓存在类里面，而非每次获取都要重新计算一次
        int size = 0;
        for (int i = 0; i < items.length; i++) {
            size += items[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // code done
        // 合并两个TupleDesc，返回一个新的TupleDesc
        int totalNum = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[totalNum];
        String[] fieldAr = new String[totalNum];
        for (int i = 0; i < td1.numFields(); i++) {
            typeAr[i] = td1.items[i].fieldType;
            fieldAr[i] = td1.items[i].fieldName;
        }
        int td1NumFields = td1.numFields();
        for (int i = 0; i < td2.numFields(); i++) {
            typeAr[i + td1NumFields] = td2.items[i].fieldType;
            fieldAr[i + td1NumFields] = td2.items[i].fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // code done
        // 判断两个TupleDesc是否相等，相等返回true，不相等返回false
        // 判断o是不是空
        if (o == null) {
            return false;
        }
        // 判断o是不是TupleDesc类型
        if (o.getClass() != this.getClass()) {
            return false;
        }
        // 将o转换为TupleDesc类型
        TupleDesc td = (TupleDesc) o;
        // 判断两个TupleDesc的item数量是否相等
        if (td.numFields() != this.numFields()) {
            return false;
        }
        // 判断两个TupleDesc的对应下标的item类型是否相等
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.items[i].fieldType.equals(td.items[i].fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // code done
        String str = "";
        for (int i = 0; i < items.length - 1; i++) {
            str += items[i].fieldType.toString() + "(" + items[i].fieldName + ")" + ", ";
        }
        str += items[items.length - 1].fieldType.toString() + "(" + items[items.length - 1].fieldName + ")";
        return str;
    }
}
