package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * 使用一个谓词比较两个元组的字段
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int fieldIndex1;
    private int fieldIndex2;
    private Predicate.Op op;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); either
     *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *               Predicate.Op.LESS_THAN_OR_EQ
     *               可使用的op包括>,<,=,>=,<=
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // TODO: some code goes here
        this.fieldIndex1 = field1;
        this.fieldIndex2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 应用谓词到给定的元组，比较可以通过字段的compare方法进行
     * 用于Join，可以是简单的nested-loop join
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // TODO: some code goes here
        if (t1 == null || t2 == null) return t1 == t2;
        Field f1 = t1.getField(fieldIndex1);
        Field f2 = t2.getField(fieldIndex2);
        return f1.compare(op, f2);
    }

    public int getField1() {
        // TODO: some code goes here
        return fieldIndex1;
    }

    public int getField2() {
        // TODO: some code goes here
        return fieldIndex2;
    }

    public Predicate.Op getOperator() {
        // TODO: some code goes here
        return op;
    }
}
