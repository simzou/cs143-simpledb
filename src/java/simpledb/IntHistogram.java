package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] buckets;
    private int numBuckets;
    private int mod;
    private int totalValues;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.numBuckets = buckets;
        this.mod = (int) Math.ceil((double) (max - min)/buckets);
        this.totalValues = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        // div should be casted back to an int representing array index (bucket num)
        int bucket = v/this.mod;
        this.buckets[bucket]++;
        this.totalValues++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        String operator = op.toString();
        switch(operator) {
            case "=":
            case "LIKE":
                return estimateSelectivityEquals(v);
            case ">":
            case "<":
                return estimateSelectivityInequality(op,v);
            case ">=":
            case "<=":
                return (estimateSelectivityEquals(v) + estimateSelectivityInequality(op,v));
            case "<>":
                return (estimateSelectivityInequality(Predicate.Op.GREATER_THAN,v) + estimateSelectivityInequality(Predicate.Op.LESS_THAN,v));
            default:
                return -1.0; 
        }
    }

    private double estimateSelectivityEquals(int v) {
        int bucket = v/this.mod;
        int width = this.mod; 
        int height = this.buckets[bucket];
        return (height/width)/this.totalValues;
    }

    private double estimateSelectivityInequality(Predicate.Op op, int v) {
        int bucket = v/this.mod;
        int width = this.mod; 
        int height = this.buckets[bucket];
        double bucket_f = 0.0;
        double selectivity = 0.0;
        String operator = op.toString();
        switch(operator) {
            case ">":
                int b_right = (bucket+1) * this.mod;
                bucket_f = (b_right - v)/width;
                selectivity = (height/this.totalValues) * bucket_f;
                for (int i=bucket+1;i<this.numBuckets;i++) {
                    selectivity += this.buckets[i]/this.totalValues;    
                }
                return selectivity;
            case "<":
                int b_left = bucket * this.mod;
                bucket_f = (v - b_left)/width;
                selectivity = (height/this.totalValues) * bucket_f;
                for (int i=0;i<bucket;i++) {
                    selectivity += this.buckets[i]/this.totalValues;    
                }
                return selectivity;
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
