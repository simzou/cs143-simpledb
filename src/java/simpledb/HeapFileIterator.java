package simpledb;

public class HeapFileIterator implements DbFileIterator {
    public HeapFileIterator(int HeapFileId, TransactionId tid, int numPages) {
        this.HeapFileId = HeapFileId;
        this.numPages = numPages;
        this.tid = tid;
        this.opened = false;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.curPageNum = 0;
        this.curPageId = new HeapPageId(this.HeapFileId, this.curPageNum);
        this.curPage = BufferPool.getPage(this.tid, this.curPageId, Permissions.READ_WRITE);
        this.curIterator = this.curPage.iterator();
        this.opened = true;
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.opened) {
            if (this.curPageNum < this.numPages-1 || this.curIterator.hasNext()) {
                return true;
            } else {
                return false;
            }
        } else {
            throw new DbException();
        }
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

        if (this.opened) {
            if (this.hasNext()) {
                if (this.curIterator.hasNext()) {
                    return this.curIterator.next();
                } else {
                    this.curPageNum++;
                    this.curPageId = new HeapPageId(this.HeapFileId, curPageNum);
                    this.curPage = BufferPool.getPage(this.tid, this.curPageId, Permissions.READ_WRITE);
                    this.curIterator = this.curPage.iterator();
                    return this.curIterator.next();
                }
            } else {
                throw new NoSuchElementException();
            }
        } else {
            throw new DbException();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (this.opened) {
            this.open();
        } else {
            throw new DbException();
        }
    }

    public void close() {
        this.opened = false;
    }

    private boolean opened;
    private int HeapFileId; 
    private TransactionId tid;
    private int curPageNum;
    private PageId curPageId;
    private Page curPage;
    private Iterator<Tuple> curIterator;
    private int numPages; 
}
