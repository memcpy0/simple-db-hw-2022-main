package simpledb.storage;

import simpledb.common.DbException;
import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable, Iterable<Field> {

    private static final long serialVersionUID = 1L;

    /**
     * 该元组引用一个TupleDesc对象描述包含的所有字段，包括字段的类型和名称
     */
    private TupleDesc tupleDesc;

    /**
     * 元组中的所有字段，包括字段的类型和值等
     */
    private ArrayList<simpledb.storage.Field> fields;

    /**
     * 记录该元组在哪一个表的哪一页的特定位置
     */
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(simpledb.storage.TupleDesc td) {
        // TODO: some code goes here
        tupleDesc = td;
        fields = new ArrayList<>();

        // 按照TupleDesc中的描述创建一个新元组
        for (TupleDesc.TDItem tdItem : td) {
            Type fieldType = tdItem.fieldType;
            if (Type.INT_TYPE.equals(fieldType)) { // 是int类型
                fields.add(new IntField(0));
            } else if (Type.STRING_TYPE.equals(fieldType)) { // 是固定长度的String
                fields.add(new StringField("", 0));
            }
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public simpledb.storage.TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public simpledb.storage.RecordId getRecordId() {
        // TODO: some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(simpledb.storage.RecordId rid) {
        // TODO: some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // TODO: some code goes here
        // 修改某个字段为f, 如果字段的类型不同，可能要修改tupleDesc中的类型数组
        Type fieldType = f.getType();
        Type oldType = tupleDesc.getFieldType(i);
        if (!oldType.equals(fieldType)) tupleDesc.setFieldType(i, fieldType);
        fields.set(i, f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // TODO: some code goes here
        Field f = null;
        try {
            f = fields.get(i);
        } catch (IndexOutOfBoundsException ex) {
            ex.printStackTrace();
        }
        return f;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // TODO: some code goes here
//        throw new UnsupportedOperationException("Implement this");
        StringBuilder sb = new StringBuilder();
        boolean flag = false;
        for (Field field : fields) {
            if (flag) sb.append("\t");
            sb.append(field); // 获取其字符串表示
            flag = true;
        }
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // TODO: some code goes here
        return new Itr();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(simpledb.storage.TupleDesc td) {
        // TODO: some code goes here
        tupleDesc = td;
    }

    @Override
    public Iterator<Field> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<Field> {
        int cursor;     // index of next element to return
        int lastRet = -1; // index of last element to returned; -1 if no such

        // prevent creating a synthetic constructor
        Itr() {}

        @Override
        public boolean hasNext() { return cursor != fields.size(); }

        @Override
        public Field next() {
            int i = cursor;
            if (i >= fields.size()) throw new NoSuchElementException();
            cursor = i + 1;
            return fields.get(lastRet = i);
        }

        @Override
        public void remove() {
            if (lastRet < 0) throw new IllegalArgumentException();
            try {
                fields.remove(lastRet);
                cursor = lastRet;
                lastRet = -1;
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public void forEachRemaining(Consumer<? super Field> action) {
            Objects.requireNonNull(action);
            final int size = fields.size();
            int i = cursor;
            if (i < size) {
                for (; i < size; ++i)
                    action.accept(fields.get(i));
                cursor = i;
                lastRet = i - 1;
            }
        }
    }
}