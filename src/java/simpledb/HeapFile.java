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
    	RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
    	PageId pid = page.getId();
    	int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
    	raf.seek(offset);
    	raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
    	raf.close();
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
    		HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (hpage.getNumEmptySlots() > 0)
        		return hpage;
    	}
    	return null;
    }
    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // ArrayList<Page> modifiedPages = new ArrayList<Page>();
        HeapPage hpage = getFreePage(tid);
        if (hpage != null)
        {
        	hpage.insertTuple(t);
        	return new ArrayList<Page> (Arrays.asList(hpage));
        }
        
        // no empty pages found, so create a new one
        HeapPageId newHeapPageId = new HeapPageId(this.getId(), this.numPages());
        HeapPage newHeapPage = new HeapPage(newHeapPageId, HeapPage.createEmptyPageData());
        newHeapPage.insertTuple(t);
        
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        int offset = BufferPool.PAGE_SIZE * this.numPages();
        raf.seek(offset);
        byte[] newHeapPageData = newHeapPage.getPageData();
        raf.write(newHeapPageData, 0, BufferPool.PAGE_SIZE);
        raf.close();
        
        return new ArrayList<Page> (Arrays.asList(newHeapPage));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        hpage.deleteTuple(t);
        return new ArrayList<Page> (Arrays.asList(hpage));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    /**
     * Helper class that implements the Java Iterator for tuples on a HeapFile
     */
    class HeapFileIterator extends AbstractDbFileIterator {

    	/**
    	 * An iterator to tuples for a particular page.
    	 */
        Iterator<Tuple> m_tupleIt;
       
        /**
         * The current number of the page this class is iterating through.
         */
        int m_currentPageNumber;

        /**
         * The transaction id for this iterator.
         */
        TransactionId m_tid;
        
        /**
         * The underlying heapFile.
         */
        HeapFile m_heapFile;

        /**
         * Set local variables for HeapFile and Transactionid
         * @param hf The underlying HeapFile.
         * @param tid The transaction ID.
         */
        public HeapFileIterator(HeapFile hf, TransactionId tid) {            
        	m_heapFile = hf;
            m_tid = tid;
        }

        /**
         * Open the iterator, must be called before readNext.
         */
        public void open() throws DbException, TransactionAbortedException {
            m_currentPageNumber = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            
        	// If the current tuple iterator has no more tuples.
        	if (m_tupleIt != null && !m_tupleIt.hasNext()) {	
                m_tupleIt = null;
            }

        	// Keep trying to open a tuple iterator until we find one of run out of pages.
            while (m_tupleIt == null && m_currentPageNumber < m_heapFile.numPages() - 1) {
                m_currentPageNumber++;		// Go to next page.
                
                // Get the iterator for the current page
                HeapPageId currentPageId = new HeapPageId(m_heapFile.getId(), m_currentPageNumber);
                                
                HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(m_tid,
                        currentPageId, Permissions.READ_ONLY);
                m_tupleIt = currentPage.iterator();
                
                // Make sure the iterator has tuples in it
                if (!m_tupleIt.hasNext())
                    m_tupleIt = null;
            }

            // Make sure we found a tuple iterator
            if (m_tupleIt == null)
                return null;
            
            // Return the next tuple.
            return m_tupleIt.next();
        }

        /**
         * Rewind closes the current iterator and then opens it again.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Close the iterator, which resets the counters so it can be opened again.
         */
        public void close() {
            super.close();
            m_tupleIt = null;
            m_currentPageNumber = Integer.MAX_VALUE;
        }
    }

}

