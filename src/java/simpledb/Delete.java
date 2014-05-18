package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_transactionId;
    private DbIterator m_it;
    private TupleDesc m_resultTupleDesc;
    private boolean m_deleted;

    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	m_transactionId = t;
    	m_it = child;
    	m_deleted = false;

    	String[] names = new String[] {"Deleted"};
    	Type[] types = new Type[] {Type.INT_TYPE};
    	m_resultTupleDesc = new TupleDesc(types, names);

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	m_it.open();
    	m_deleted = false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	m_it.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_it.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (m_deleted) return null;
    	int deletedCount = 0;
    	while (m_it.hasNext())
    	{
    		Tuple tup = m_it.next();
    		try 
    		{
        		Database.getBufferPool().deleteTuple(m_transactionId, tup);    			
    		}
    		catch (IOException e)
    		{
    			throw new DbException("IO Exception on tuple deletion");
    		}
    		deletedCount++;
    	}
    	Tuple resultTuple = new Tuple(m_resultTupleDesc);
    	resultTuple.setField(0, new IntField(deletedCount));
    	m_deleted = true;
    	return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {m_it};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	m_it = children[0];
    }

}
