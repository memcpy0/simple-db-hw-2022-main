package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static simpledb.common.Type.INT_TYPE;
import static simpledb.common.Type.STRING_TYPE;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Iterable<TupleDesc.TDItem> {
    private static final long serialVersionUID = 1L;

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

        public boolean equals(Object o) {
            // 判断传入的对象是否为当前对象的引用
            if (this == o) return true;
            // 判断传入的对象是否为null 或者不是当前对象的实例
            if (o == null || getClass() != o.getClass()) return false;

            // 将传入的对象转换为当前对象类型
            TDItem other = (TDItem) o;
            // 实现自定义逻辑来比较对象的属性是否相等
            return Objects.equals(fieldType, other.fieldType) &&
                    Objects.equals(fieldName, other.fieldName);
        }

        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * TDItem的列表，每个TDItem描述了元组中一个字段的类型和名称
     */
    private ArrayList<TDItem> tdItems;

    private class Itr implements Iterator<TDItem> {
        int cursor;     // index of next element to return
        int lastRet = -1; // index of last element to returned; -1 if no such

        // prevent creating a synthetic constructor
        Itr() {}

        // 判断是否还有下一个元素
        @Override
        public boolean hasNext() { return cursor != tdItems.size(); }

        // 返回下一个元素
        @Override
        public TDItem next() {
            int i = cursor;
            if (i >= tdItems.size())
                throw new NoSuchElementException();
            cursor = i + 1;
            return tdItems.get(lastRet = i);
        }

        // 删除当前元素
        @Override
        public void remove() {
            if (lastRet < 0)
                throw new IllegalArgumentException();
            try {
                tdItems.remove(lastRet);
                cursor = lastRet; // cursor = lastRet+1, 当lastRet对应的元素被删除, cursor指向的元素前移
                lastRet = -1;
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public void forEachRemaining(Consumer<? super TDItem> action) {
            Objects.requireNonNull(action);
            final int size = tdItems.size();
            int i = cursor;
            if (i < size) {
                for (; i < size; ++i)
                    action.accept(tdItems.get(i));
                // update once at end to reduce heap write traffic
                cursor = i;
                lastRet = i - 1;
            }
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // TODO: some code goes here
        return new Itr();
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // TODO: some code goes here
        // 长度必须相等
        assertEquals(typeAr.length, fieldAr.length);
        tdItems = new ArrayList<>();

        for (int i = 0; i < typeAr.length; ++i) {
            tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // TODO: some code goes here
        tdItems = new ArrayList<>();

        for (Type type : typeAr) {
            tdItems.add(new TDItem(type, "")); // 匿名字段的名称为空串
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // TODO: some code goes here
        if (i < 0 || i >= tdItems.size())
            throw new NoSuchElementException("tdItem index invalid");
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // TODO: some code goes here
        if (i < 0 || i >= tdItems.size())
            throw new NoSuchElementException("tdItem index invalid");
        return tdItems.get(i).fieldType;
    }

    /**
     * Sets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to set the type of. It must be a valid
     *          index.
     * @return
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public void setFieldType(int i, Type fieldType) throws NoSuchElementException {
        // TODO: some code goes here
        if (i < 0 || i >= tdItems.size())
            throw new NoSuchElementException("tdItem index invalid");
        TDItem item = tdItems.get(i);
        tdItems.set(i, new TDItem(fieldType, item.fieldName)); // 设置字段的新类型
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        // TODO: some code goes here
        // 名称为空的字段不存在，匿名字段为空串
        if (name == null)
            throw new NoSuchElementException("null name cannot be found");
        // 遍历看能否找到名称为name的field
        for (int i = 0; i < tdItems.size(); ++i) {
            TDItem tdItem = tdItems.get(i);
            if (name.equals(tdItem.fieldName))
                return i;
        }
        throw new NoSuchElementException("don't find field with given name " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // TODO: some code goes here
        // 一个tuple的数据占用的大小
        int tupleSize = 0;
        for (TDItem tdItem : tdItems) {
            if (INT_TYPE.equals(tdItem.fieldType)) tupleSize += INT_TYPE.getLen();
            else if (STRING_TYPE.equals(tdItem.fieldType)) tupleSize += STRING_TYPE.getLen();
        }
        return tupleSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // TODO: some code goes here
        int totalLen = td1.numFields() + td2.numFields();
        Type[] totalTypes = new Type[totalLen];
        String[] totalNames = new String[totalLen];
        int k = 0;
        for (TDItem tdItem : td1.tdItems) {
            totalTypes[k] = tdItem.fieldType;
            totalNames[k++] = tdItem.fieldName;
        }
        for (TDItem tdItem : td2.tdItems) {
            totalTypes[k] = tdItem.fieldType;
            totalNames[k++] = tdItem.fieldName;
        }
        return new TupleDesc(totalTypes, totalNames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // TODO: some code goes here
        // 判断传入的对象是否为当前对象的引用
        if (this == o) return true;
        // 判断传入的对象是否为null 或者不是当前对象的实例
        if (o == null || getClass() != o.getClass()) return false;

        // 将传入的对象转换为当前对象类型
        TupleDesc other = (TupleDesc) o;
        // 实现自定义逻辑来比较对象的属性是否相等
        if (tdItems == other.tdItems) return true; // 都引用同个对象
        if (other.tdItems.size() != tdItems.size()) return false;
        for (int i = 0, n = tdItems.size(); i < n; ++i) {
            if (!tdItems.get(i).equals(other.tdItems.get(i))) return false;
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
        // TODO: some code goes here
        StringBuilder sb = new StringBuilder();
        boolean flag = false;
        for (TDItem tdItem : tdItems) {
            if (flag) sb.append(',');
            sb.append(tdItem.fieldType).append('(').append(tdItem.fieldName).append(')');
            flag = true;
        }
        return sb.toString();
    }
}
