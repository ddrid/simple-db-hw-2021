package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
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
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
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
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid.getTableId() != getId() || pid.getPageNumber() >= numPages())
            throw new IllegalArgumentException("This page does not exist in this file.");
        try {
            RandomAccessFile fileInputStream = new RandomAccessFile(file, "r");
            int pageSize = BufferPool.getPageSize();
            byte[] bytes = new byte[pageSize];
            fileInputStream.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            fileInputStream.read(bytes, 0, pageSize);
            return new HeapPage((HeapPageId) pid, bytes);
        } catch (Exception e) {
            throw new IllegalArgumentException("Read page exception: " + e);
        }
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
        return (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
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
        return new DbFileIterator() {

            private boolean opened = false;
            private int pageIndex = -1;
            private int totalPages;
            private int tableId;
            private Iterator<Tuple> pageIterator;
            private BufferPool bufferPool;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                pageIndex = -1;
                totalPages = numPages();
                tableId = getId();
                bufferPool = Database.getBufferPool();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!opened) return false;
                if (pageIterator == null || !pageIterator.hasNext()) {
                    pageIndex++;
                    if (pageIndex >= totalPages) return false;
                    HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableId, pageIndex), null);
                    pageIterator = page.iterator();
                }
                return pageIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened) throw new NoSuchElementException();
                if (pageIterator == null || !pageIterator.hasNext()) {
                    pageIndex++;
                    if (pageIndex >= totalPages) throw new NoSuchElementException();
                    HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableId, pageIndex), null);
                    pageIterator = page.iterator();
                }
                return pageIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageIndex = -1;
                pageIterator = null;
            }

            @Override
            public void close() {
                opened = false;
            }
        };
    }

}

