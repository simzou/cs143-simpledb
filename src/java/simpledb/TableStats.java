package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
	private int iocostperpage;
	private int numTuples;
	private HashMap<String, Integer> maxs;
	private HashMap<String, Integer> mins;
	private HashMap<String, IntHistogram> intHistograms;
	private HashMap<String, StringHistogram> stringHistograms;
	private DbFile file;
	private TupleDesc td;
	
    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	maxs = new HashMap<String, Integer>();
    	mins = new HashMap<String, Integer>();
    	intHistograms = new HashMap<String, IntHistogram>();
    	stringHistograms = new HashMap<String, StringHistogram>();
    	
    	numTuples = 0;
    	file = Database.getCatalog().getDatabaseFile(tableid);
    	TransactionId tid = new TransactionId();
    	DbFileIterator iter = file.iterator(tid);
    	iocostperpage = ioCostPerPage;
    	this.td = Database.getCatalog().getTupleDesc(tableid);
    	setMinsAndMaxs(iter, td);
    
    	// initialize histograms
    	for (int i = 0; i < td.numFields(); i++)
    	{
    		String fieldname = td.getFieldName(i);
    		switch(td.getFieldType(i))
    		{
    		case INT_TYPE:
    			IntHistogram ihist = new IntHistogram(NUM_HIST_BINS, mins.get(fieldname), maxs.get(fieldname));
    			this.intHistograms.put(fieldname, ihist);
    			break;
    		case STRING_TYPE:
    			StringHistogram shist = new StringHistogram(NUM_HIST_BINS);
    			this.stringHistograms.put(fieldname, shist);
    			break;
    		}
    	}
    	
    	// populate histograms
    	populateHistograms(iter, td);

    }

    private void populateHistograms(DbFileIterator iter, TupleDesc td)
    {
    	Tuple currTup;
    	try {
			iter.open();
			while (iter.hasNext())
			{
				currTup = iter.next();
				for (int i = 0; i < td.numFields(); i++)
				{
					String fieldname = td.getFieldName(i);
					switch (td.getFieldType(i))
					{
					case INT_TYPE:
						int intTupleValue = ((IntField) currTup.getField(i)).getValue();
						this.intHistograms.get(fieldname).addValue(intTupleValue);
						break;
					case STRING_TYPE:
						String stringTupleValue = ((StringField) currTup.getField(i)).getValue();
						this.stringHistograms.get(fieldname).addValue(stringTupleValue);
						break;
					}
				}
			}
			iter.close();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private void setMinsAndMaxs(DbFileIterator iter, TupleDesc td)
    {
    	Tuple currTup;
    	try {
			iter.open();
	    	while (iter.hasNext())
	    	{
	    		currTup = iter.next();
	    		numTuples++;
	    		for (int i = 0; i < td.numFields(); i++)
	    		{	    
	    			String fieldname = td.getFieldName(i);
	    			switch (td.getFieldType(i))
	    			{
	    			case INT_TYPE:
	    				int fieldvalue = ((IntField) currTup.getField(i)).getValue();
	    				if (!this.maxs.containsKey(fieldname))
	    					maxs.put(fieldname, fieldvalue);
	    				else 
	    				{
	    					int currentMax = maxs.get(fieldname);
	    					int newMax = (currentMax > fieldvalue) ? currentMax : fieldvalue;
	    					maxs.put(fieldname, newMax);
	    				}
	    				if (!this.mins.containsKey(fieldname))
	    					mins.put(fieldname, fieldvalue);
	    				else 
	    				{
	    					int currentMin = mins.get(fieldname);
	    					int newMin = (currentMin < fieldvalue) ? currentMin : fieldvalue;
	    					mins.put(fieldname, newMin);
	    				}
	    				break;
	    			case STRING_TYPE:
	    				break;
	    			}
	    		}
	    	}
	    	iter.close();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        // replace this.file with the db_file variable
        // replace this.iocostperpage
        return ((HeapFile)this.file).numPages() * this.iocostperpage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // Assuming we can get number of tuples from the scan
        return (int) (this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        // this.intHistograms = int type histogram from scan
        // this.stringHistograms = string type histogram from scan
    	String fieldname = this.td.getFieldName(field);
        if (constant.getType().equals(Type.STRING_TYPE)) {
            String value = ((StringField) constant).getValue();
            StringHistogram hist = this.stringHistograms.get(fieldname);
            return hist.estimateSelectivity(op,value);
        } else {
            int value = ((IntField) constant).getValue();
            IntHistogram hist =  this.intHistograms.get(fieldname);
            return hist.estimateSelectivity(op,value);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
