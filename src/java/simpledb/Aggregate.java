package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator m_tupleIterator;
    private int m_aggregateFieldIndex;
    private int m_groupByFieldIndex;
    private Aggregator.Op m_op;
    
    private Aggregator m_aggregator;
    private DbIterator m_aggregateIterator;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	m_tupleIterator = child;
    	m_aggregateFieldIndex = afield;
    	m_groupByFieldIndex = gfield;
    	m_op = aop;
    	m_aggregateIterator = null;
    	
    	Type groupByType;
    	
    	if (m_groupByFieldIndex == Aggregator.NO_GROUPING)
    		groupByType = null;
    	else 
    		groupByType = m_tupleIterator.getTupleDesc().getFieldType(m_groupByFieldIndex);
    	
    	Type aggregateType = m_tupleIterator.getTupleDesc().getFieldType(m_aggregateFieldIndex);
    	switch(aggregateType)
    	{
    		case INT_TYPE: 
    			m_aggregator = new IntegerAggregator(m_groupByFieldIndex, groupByType, m_aggregateFieldIndex ,m_op);
    			break;
    		case STRING_TYPE:
    			m_aggregator = new StringAggregator(m_groupByFieldIndex, groupByType, m_aggregateFieldIndex ,m_op);
    			break;
    		default:
    			assert(false);
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return    	return m_aggregateIterator.next();

     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return m_groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if (this.groupField() == Aggregator.NO_GROUPING){ return null; }
    	return m_tupleIterator.getTupleDesc().getFieldName(this.groupField());
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    	return m_aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    	return m_tupleIterator.getTupleDesc().getFieldName(this.aggregateField());
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    	return m_op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	super.open();
    	m_tupleIterator.open();
    	while (m_tupleIterator.hasNext())
    	{
    		m_aggregator.mergeTupleIntoGroup(m_tupleIterator.next());
    	}
    	m_aggregateIterator = m_aggregator.iterator();
    	m_aggregateIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	return m_aggregateIterator.hasNext() ? m_aggregateIterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	m_aggregateIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	return m_tupleIterator.getTupleDesc();
    }

    public void close() {
	// some code goes here
    	super.close();
    	m_tupleIterator.close();
    	m_aggregateIterator.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
    	return new DbIterator[] {m_aggregateIterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    	m_aggregateIterator = children[0];
    }
    
}
