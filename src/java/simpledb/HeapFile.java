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
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private int id;
    private File file;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.id = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the Frile backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        return this.id;
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile f = new RandomAccessFile(this.file,"r");
            int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            if (offset + BufferPool.PAGE_SIZE > f.length()) {
                System.err.println("Page offset exceeds max size, error!");
                System.exit(1);
            }
            f.seek(offset);
            f.readFully(data);
            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            throw new IllegalArgumentException();
        } catch (IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
            throw new IllegalArgumentException();
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
        // some code goes here
        return (int) Math.ceil(this.file.length()/BufferPool.PAGE_SIZE);
    }

    private HeapPage getFreePage(TransactionId tid) throws TransactionAbortedException, DbException
    {
    	for (int i = 0; i < this.numPages(); i++)
    	{
    		PageId pid = new HeapPageId(this.getId(), i);
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (page.getNumEmptySlots() > 0)
        		return page;
    	}
    	return null;
    }
    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // ArrayList<Page> modifiedPages = new ArrayList<Page>();
        HeapPage page = getFreePage(tid);
        if (page != null)
        {
        	page.insertTuple(t);
        	return new ArrayList<Page> (Arrays.asList(page));
        }
        
        // no empty pages found, so create a new one
        HeapPageId newHeapPageId = new HeapPageId(this.getId(), this.numPages());
        HeapPage newHeapPage = new HeapPage(newHeapPageId, HeapPage.createEmptyPageData());
        newHeapPage.insertTuple(t);
        
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(BufferPool.getPageSize() * this.numPages());
        raf.write(newHeapPage.getPageData(), 0, BufferPool.getPageSize());
        raf.close();
        
        return new ArrayList<Page> (Arrays.asList(newHeapPage));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page> (Arrays.asList(page));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this.id,tid,this.numPages());
    }

}

