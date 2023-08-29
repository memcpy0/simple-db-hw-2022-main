package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * HeapFile是一个DbFile的实现，用于以无特定顺序存储一组元组。
 * 元组存储在页面上，每个页面具有固定的大小，文件只是这些页面的集合。
 * HeapFile与HeapPage密切配合。HeapPages的格式在HeapPage构造函数中描述
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc tupleDesc;

    /**
     * 唯一标识该HeapFile的ID
     */
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.tupleDesc = td;
        tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableId somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 该方法返回一个唯一标识该HeapFile的ID。实现说明：您需要在某处生成tableId，
     * 以确保每个HeapFile具有“唯一的ID”，并且对于特定的HeapFile，始终返回相同的值
     * 我们建议使用底层堆文件的绝对文件名进行哈希处理，即f.getAbsoluteFile().hashCode()。
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 返回存储在DbFile中的表的TupleDesc
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return tupleDesc;
    }

    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    // see DbFile.java for javadocs
    // TODO: 参考改进
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        if (pid.getTableId() != tableId)
            throw new IllegalArgumentException("read page from wrong table");
        // 根据要读取的页号和页大小，获取文件中的位置
        int pageNo = pid.getPageNumber();
        int len = BufferPool.getPageSize();
        int position = pageNo * len;

        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            // 将文件中的指针定位到指定位置
            raf.seek(position);
            // 创建字节数组来存储读取的数据
            byte[] buffer = new byte[len];
            raf.read(buffer, 0, len);
            return new HeapPage((HeapPageId) pid, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) Math.ceilDiv(f.length(), BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(this, tid);
    }

    /**
     * 写在内部类的原因是：DbFileIterator is the iterator interface that all SimpleDB Dbfile should implement
     * 静态内部类是被声明为静态的，而普通内部类不是。
     * 静态内部类可以访问外部类的静态成员，不能直接访问外部类的实例成员；而普通内部类可以访问外部类的实例成员。
     * 静态内部类可以直接通过外部类的类名访问，无需创建外部类的实例。而普通内部类需要先创建外部类的实例，然后通过外部类的实例访问。
     * 静态内部类的生命周期与外部类无关，它可以在没有外部类实例的情况下存在。普通内部类的生命周期依赖于外部类的实例，只能在有外部类实例的情况下创建。
     * 静态内部类通常用于封装与外部类相关但与实例无关的功能，可以像一个独立的类一样使用。普通内部类通常用于访问外部类的实例成员和实现与外部类紧密关联的功能。
     */
    private static class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;

        /**
         * 用于迭代HeapFile中Tuple的迭代器
         */
        private Iterator<Tuple> tupleIterator;
        private int index;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.heapFile = hf;
            this.tid = tid;
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            tupleIterator = getTupleIterator(index);
        }

        /**
         * 返回一个某页的元组迭代器
         * @param pageNo
         * @return
         * @throws TransactionAbortedException
         * @throws DbException
         */
        private Iterator<Tuple> getTupleIterator(int pageNo)
            throws TransactionAbortedException, DbException {
            // 页号存在
            if (pageNo >= 0 && pageNo < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                        Permissions.READ_ONLY);
                return page.iterator(); // 返回对该页的迭代器，从而实现对元组的迭代！
            } else {
                throw new DbException(String.format("page[%d] doesn't exist in heapFile[%d]", pageNo, heapFile.getId()));
            }
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) return false;
            while (!tupleIterator.hasNext()) { // 如果当前页的元组迭代器用完了
                ++index;
                if (index < heapFile.numPages()) // 判断是否迭代完所有页
                    tupleIterator = getTupleIterator(index);
                else return false;
            }
            return true;
        }
        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null || !tupleIterator.hasNext())
                throw new NoSuchElementException();
            return tupleIterator.next(); // 返回迭代器中的下个元组
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close(); open(); // 先关闭再打开
        }
        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            tupleIterator = null;
            index = 0; // 重置回第一页
        }
    }
}

