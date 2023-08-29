package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    /**
     * 返回一个新的元组，使用td作为描述符
     */
    private final TupleDesc td;
    private final List<Integer> outFieldIds;

    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * 会接收一个子operator来读取元组并应用projection
     * to and a list of fields in output tuple
     * 还会接收一个输出字段的ID列表
     *
     * @param fieldList The ids of the fields child's tupleDesc to project out
     * @param typesList the types of the fields in the final projection
     * @param child     The child operator
     */
    public Project(List<Integer> fieldList, List<Type> typesList,
                   OpIterator child) {
        this(fieldList, typesList.toArray(new Type[]{}), child);
    }

    public Project(List<Integer> fieldList, Type[] types,
                   OpIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDesc childtd = child.getTupleDesc();

        for (int i = 0; i < fieldAr.length; i++) { // 从孩子tupleDesc中获取各字段的名称
            fieldAr[i] = childtd.getFieldName(fieldList.get(i));
        }
        td = new TupleDesc(types, fieldAr);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the child
     * operator, projecting out the fields from the tuple
     * 真正实现功能的代码
     *
     * @return The next tuple, or null if there are no more tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!child.hasNext()) return null;
        Tuple t = child.next();
        Tuple newTuple = new Tuple(td); // 投影后要输出的元组
        newTuple.setRecordId(t.getRecordId()); // 元组的位置
        for (int i = 0; i < td.numFields(); i++) { // 设置字段
            newTuple.setField(i, t.getField(outFieldIds.get(i)));
        }
        return newTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
