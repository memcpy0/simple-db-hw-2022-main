package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        /**
         * 等于，不等于，更大，更小，大于等于，小于等于
         */
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private int field;
    private Op op;
    private Field operand;
    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against. 和元组的哪个字段做比较
     * @param op      operation to use for comparison 比较操作
     * @param operand field value to compare passed in tuples to 要比较的值
     */
    public Predicate(int field, Op op, Field operand) {
        // TODO: some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        // TODO: some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        // TODO: some code goes here
        return op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        // TODO: some code goes here
        return operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // TODO: some code goes here
        Field f = t.getField(field); // 某个元组的第field个字段
        return f.compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // TODO: some code goes here
        StringBuilder sb = new StringBuilder();
        sb.append("f = ").append(field).append(" ");
        sb.append("op = ").append(op).append(" ");
        sb.append("operand = ").append(operand);
        return sb.toString();
    }
}
