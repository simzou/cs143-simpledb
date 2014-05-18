package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int m_groupByFieldIndex;
    private Type m_groupByFieldType;
    private int m_aggregateFieldIndex;
    private Op m_op;
    private HashMap<Field,Integer> m_count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	m_groupByFieldIndex = gbfield;
    	m_groupByFieldType = gbfieldtype;
    	m_aggregateFieldIndex = afield;
    	m_op = what;
    	assert(m_op == Op.COUNT);
    	m_count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field tupleGroupByField = (m_groupByFieldIndex == Aggregator.NO_GROUPING) ? null : tup.getField(m_groupByFieldIndex);
    	
    	if (!m_count.containsKey(tupleGroupByField))
    	{
    		m_count.put(tupleGroupByField, 0);
    	}
    	
    	int currentCount = m_count.get(tupleGroupByField);
    	m_count.put(tupleGroupByField, currentCount+1);

    }
    
    private TupleDesc createGroupByTupleDesc()
    {
    	String[] names;
    	Type[] types;
    	if (m_groupByFieldIndex == Aggregator.NO_GROUPING)
    	{
    		names = new String[] {"aggregateValue"};
    		types = new Type[] {Type.INT_TYPE};
    	}
    	else
    	{
    		names = new String[] {"groupValue", "aggregateValue"};
    		types = new Type[] {m_groupByFieldType, Type.INT_TYPE};
    	}
    	return new TupleDesc(types, names);
    }
    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupledesc = createGroupByTupleDesc();
    	Tuple addMe;
    	for (Field group : m_count.keySet())
    	{
    		int aggregateVal = m_count.get(group);
    		addMe = new Tuple(tupledesc);
    		if (m_groupByFieldIndex == Aggregator.NO_GROUPING){
    			addMe.setField(0, new IntField(aggregateVal));
    		}
    		else {
        		addMe.setField(0, group);
        		addMe.setField(1, new IntField(aggregateVal));    			
    		}
    		tuples.add(addMe);
    	}
    	return new TupleIterator(tupledesc, tuples);
    }

}
