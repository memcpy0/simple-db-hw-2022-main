package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * pageId表示哪张表的哪一页
     */
    private PageId pageId;

    /**
     * tupleNo表示是哪个元组
     */
    private int tupleNo;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleNo the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleNo) {
        // TODO: some code goes here
        this.pageId = pid;
        this.tupleNo = tupleNo;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // TODO: some code goes here
        return tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // TODO: some code goes here
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // TODO: some code goes here
        // 判断传入的对象是否为当前对象的引用
        if (this == o) return true;
        // 判断传入的对象是否为null 或者不是当前对象的实例
        if (o == null || getClass() != o.getClass()) return false;

        // 将传入的对象转换为当前对象类型
        RecordId other = (RecordId) o;
        // 实现自定义逻辑来比较对象的属性是否相等
        return tupleNo == other.tupleNo && Objects.equals(pageId, other.pageId);
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // TODO: some code goes here
        return Objects.hash(pageId.hashCode(), tupleNo);
    }
}
