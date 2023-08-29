package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {
    /**
     * 数据库中表的ID
     */
    private int tableId;
    /**
     * 每张表的哪一页,从0开始
     */
    private int pageNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // TODO: some code goes here
        this.tableId = tableId;
        this.pageNo = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        // TODO: some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        // TODO: some code goes here
        return pageNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // TODO: some code goes here
        return Objects.hash(tableId, pageNo);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        // TODO: some code goes here
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HeapPageId other = (HeapPageId) o;
        return tableId == other.tableId && pageNo == other.pageNo;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors. 返回的数组大小必须包含与构造函数参数数量相对应的整数数量。
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }
}
