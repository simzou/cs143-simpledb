package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_transactionId;
    private DbIterator m_it;
    private int m_tableId;
    private boolean m_inserted;
    private TupleDesc m_resultTupleDesc;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	m_transactionId = t;
    	m_it = child;
    	m_tableId = tableid;
    	m_inserted = false;
    	
    	String[] names = new String[] {"Inserted"};
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
    	m_inserted = false;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (m_inserted) return null;
    	int insertedCount = 0;
    	while (m_it.hasNext())
    	{
    		Tuple tup = m_it.next();
    		try 
    		{
        		Database.getBufferPool().insertTuple(m_transactionId, m_tableId, tup);    			
    		}
    		catch (IOException e)
    		{
    			throw new DbException("IO Exception on tuple insertion");
    		}
    		insertedCount++;
    	}
    	Tuple resultTuple = new Tuple(m_resultTupleDesc);
    	resultTuple.setField(0, new IntField(insertedCount));
    	m_inserted = true;
    	return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {m_it};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        m_it = children[0];
    }
}
