package simpledb;

import java.io.*;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private final LockManager lockManager;
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private final Object lock = new Object();
    private int Commit_cnt = 0;
    private final static Random r = new Random();

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int num_pages;
    private ConcurrentHashMap<PageId, Page> pid2page;
    private ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> tid2pids;
    private ConcurrentHashMap<PageId, TransactionId> pid2tid;
//    private Queue<PageId> LRUQueue;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        num_pages = numPages;
        pid2page = new ConcurrentHashMap<>(numPages);
        tid2pids = new ConcurrentHashMap<>();
        pid2tid = new ConcurrentHashMap<>();
        lockManager = LockManager.GetLockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if (pid == null) {
            throw new DbException("NULL PageId!");
        }

        lockManager.acquireLock(tid, pid, perm);

        if (pid2page.containsKey(pid)) {
            return pid2page.get(pid);
        } else {
            Page page = getPageFile(pid).readPage(pid);
            if (pid2page.size() == num_pages) {
                evictPage();
            }
            pid2page.put(pid, page);
            return page;
        }
    }

    private DbFile getPageFile(PageId pid) {
        int table_id = pid.getTableId();
        return Database.getCatalog().getDatabaseFile(table_id);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
//        try{
//            Thread.sleep(r.nextInt(10));
//        }catch (Exception e){}

        ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> tid2dirtypageIds
                = lockManager.getTransactionDirtiedPages();

        if (!tid2dirtypageIds.containsKey(tid)) {
            lockManager.releasePages(tid);
            return;
        }

        if (commit) {
            for (PageId pid : tid2dirtypageIds.get(tid)) {
                flushPage(pid);
                try {
                    getPage(tid, pid, Permissions.READ_WRITE).setBeforeImage();
                } catch (Exception e){}
            }
        } else {
            for (PageId pid : tid2dirtypageIds.get(tid)) {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pid2page.replace(pid, page);
                page.markDirty(false, null);
            }
        }
        lockManager.releasePages(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtypages = f.insertTuple(tid, t);
        for (Page p : dirtypages) {
            p.markDirty(true, tid);
            discardPage(p.getId());
            insertPagetoCache(tid, p);
        }
    }

    private synchronized void insertPagetoCache(TransactionId tid, Page page) throws IOException, DbException {
        if (pid2page.size() == num_pages) {
            evictPage();
        }
        pid2page.put(page.getId(), page);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = file.deleteTuple(tid, t);
        for (Page p : pages) {
            p.markDirty(true, tid);
            insertPagetoCache(tid, p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> iter = pid2page.keySet().iterator();

        while (iter.hasNext()) {
            flushPage(iter.next());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pid2page.remove(pid);
        pid2tid.remove(pid);
    }

//    private void addDirtiedFlushedPage(TransactionId dirtier, PageId pageId) {
//        if (transactionsToDirtiedFlushedPages.containsKey(dirtier)) {
//            transactionsToDirtiedFlushedPages.get(dirtier).add(pageId);
//        } else {
//            Set<PageId> dirtiedFlushedPages = new HashSet<PageId>();
//            dirtiedFlushedPages.add(pageId);
//            transactionsToDirtiedFlushedPages.put(dirtier, dirtiedFlushedPages);
//        }
//    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (pid2page.containsKey(pid)) {
            Page evict_page = pid2page.get(pid);
            if (evict_page.isDirty() != null) {
                getPageFile(pid).writePage(evict_page);
                evict_page.markDirty(false, null);
                evict_page.setBeforeImage();
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pid2page.keySet()) {
            HeapPage heapPage = (HeapPage) pid2page.get(pid);

            if (tid.equals(heapPage.isDirty())) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (pid2page.size() == 0)
            throw new DbException("Evicting page in Empty buffer!");

        PageId evict_pid = null;
        Iterator<PageId> iterator = pid2page.keySet().iterator();

        while (iterator.hasNext()) {
            PageId cur_pid = iterator.next();
            if (pid2page.get(cur_pid).isDirty() == null) {
                evict_pid = cur_pid;
                break;
            }
        }

        if (evict_pid == null)
            throw new DbException("All pages are dirty, No valid page to evict!");

        try {
            flushPage(evict_pid);
            discardPage(evict_pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

