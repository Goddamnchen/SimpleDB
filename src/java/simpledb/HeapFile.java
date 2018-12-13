package simpledb;

import com.sun.corba.se.impl.orb.DataCollectorBase;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        File f = this.file;
        return f;
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
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        int hash = file.getAbsoluteFile().hashCode();
        return hash;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        TupleDesc td = this.tupleDesc;
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int offset = pid.pageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.read(data);
            raf.close();
            Page hp = new HeapPage((HeapPageId) pid, data);
            return hp;
        } catch (FileNotFoundException fnf) {
            System.out.println(fnf.toString());
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = page.getId().pageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(offset);
            raf.write(data);
            raf.close();
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = (int) Math.ceil(file.length() / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        boolean isEmptyPage = false;
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i ++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                isEmptyPage = true;
                page.insertTuple(t);
                page.markDirty(true, tid);
                modifiedPages.add(page);
                break;
            }
        }
        if (!isEmptyPage) {
            HeapPageId newPid = new HeapPageId(getId(), numPages());
            HeapPage emptyPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
            emptyPage.insertTuple(t);
            emptyPage.markDirty(true, tid);
            // write new page to disk directly
            writePage(emptyPage);       // need insert first and then writePage()
            modifiedPages.add(emptyPage);
        }
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<>();
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        if (pid.getTableId() == getId()) {
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            page.deleteTuple(t);
            page.markDirty(true, tid);
            modifiedPages.add(page);
            return modifiedPages;
        } else {
            throw new DbException("Tuple to delete not in this HeapFile ");
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid) ;
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        TransactionId tid;
        int tupleNum;
        int tableId;
        Tuple next;
        LinkedList<Tuple> tupleQueue;

        public HeapFileIterator(TransactionId tid) {
            super();
            this.tid = tid;
            this.tableId = getId();
            this.next = null;
            this.tupleQueue = new LinkedList<>();
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            // enqueue pages
            for (int i = 0; i < numPages(); i ++ ) {
                PageId pid = new HeapPageId(tableId, i);
                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                // enqueue tuples
                Iterator<Tuple> pageIterator = hp.iterator();
                while (true) {
                    if (pageIterator.hasNext()) {
                        tupleQueue.add(pageIterator.next());
                    } else {
                        break;
                    }
                }
            }
        }

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (next == null) next = readNext();
            return next != null;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (next == null) {
                next = readNext();
                if (next == null) throw new NoSuchElementException();
            }

            Tuple result = next;
            this.tupleQueue.removeFirst();
            next = null;
            return result;
        }

        /** Reads the next tuple from the underlying source.
         @return the next Tuple in the iterator, null if the iteration is finished. */
        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            Tuple head = tupleQueue.peek();
            return head;
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /** If subclasses override this, they should call super.close(). */
        @Override
        public void close() {
            // Ensures that a future call to next() will fail
            super.close();
            //next = null;
            //this.tid = null;
            //this.tupleNum = 0;
            this.next = null;
            this.tupleQueue.clear();
        }
    }
}

