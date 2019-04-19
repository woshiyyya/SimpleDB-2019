package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tuple_desc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tuple_desc = td;

    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // HeapfileID is exactly tableID
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tuple_desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = BufferPool.getPageSize() * pid.pageNumber();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile reader = new RandomAccessFile(file, "r");
            reader.seek(offset);
            reader.read(data);
            reader.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            System.err.println("Fail when reading dbfiles");
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private int page_id;
        private BufferPool buffer_pool;

        // 这里一开始以为要用page iterator,但其实我们要遍历的是table上的tuple
        // page只是tuple存储时的数据结构；而且page.iterator返回的就是Tuple类型的iterator
        private Iterator<Tuple> cur_tuple_iter = null;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.page_id = 0;
            this.buffer_pool = Database.getBufferPool();
        }

        private Iterator<Tuple> GetTupleIterator(HeapPageId pid) throws TransactionAbortedException, DbException {
            // TODO: What's the parameter 'perm' used for??
            HeapPage page = (HeapPage) buffer_pool.getPage(tid, pid, null);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.cur_tuple_iter = GetTupleIterator(new HeapPageId(getId(), page_id));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (cur_tuple_iter == null) {
                return false;
            }
            if (cur_tuple_iter.hasNext()) {
                return true;
            }

            if (page_id + 1 >= numPages()) {
                return false;
            }
            // ptr at the last tuple of current page, but still there are non-empty pages.
            page_id++;
            cur_tuple_iter = GetTupleIterator(new HeapPageId(getId(), page_id));
            return cur_tuple_iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("DbFile EOF!");
            }
            return cur_tuple_iter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            cur_tuple_iter = null;
            page_id = 0;
        }
    }

}
