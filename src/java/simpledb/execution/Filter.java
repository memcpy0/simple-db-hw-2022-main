package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 * 实现关系选择的 operator
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;

    private OpIterator child;

    /**
     * 当前operator返回的tuple的描述符
     */
    private TupleDesc td;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // TODO: some code goes here
        this.p = p;
        this.child = child;
        this.td = child.getTupleDesc(); // 过滤后的tuple的描述符等于之前的描述符
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        child.open(); // 先开启来源的 operator
        super.open(); // 再调用super的open方法
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        child.rewind();
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 在来自child operator的元组上迭代，然后应用predicate到元组上
     * 返回满足predicate的元组
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        while (child.hasNext()) {
            Tuple t = child.next();
            if (!p.filter(t)) continue; // 不满足谓词条件
            return t;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        if (this.child != children[0])
            this.child = children[0];
    }
}
