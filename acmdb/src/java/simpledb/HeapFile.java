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
     *啊是的
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
            // TODO: some problem in reading chunks
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
        int pagesize = BufferPool.getPageSize();
        int offset = pagesize * page.getId().pageNumber();
        RandomAccessFile writer = new RandomAccessFile(file, "rw");
        writer.seek(offset);
        writer.write(page.getPageData());
        writer.close();
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
        ArrayList<Page> dirtyPages = new ArrayList<>();

        int i = 0;
        while (i < numPages()){
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0){
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                try{
                    page.insertTuple(t);
                    dirtyPages.add(page);
                    return dirtyPages;
                }catch (Exception e){
                    throw new DbException("Insertion Error");
                }
            } else{
                Database.getBufferPool().releasePage(tid, pageId);
            }
            i++;
        }


        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage newpage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        // TODO: This write might be illegal, cannot directly write page !!!
//      newpage.insertTuple(t);
//      this.writePage(newpage);
//      dirtyPages.add(newpage);

        //----new-----

        this.writePage(newpage);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        dirtyPages.add(heapPage);
        //----end-----

        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();
        if (getId() != pageId.getTableId())
            throw new DbException("Deletion on Wrong Table");

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> dirtyPages = new ArrayList<>();
        dirtyPages.add(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, getId(), numPages());
    }


}

