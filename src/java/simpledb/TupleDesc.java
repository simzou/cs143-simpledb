package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public Vector<TDItem> m_tdvec;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // return null;
    	return m_tdvec.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	m_tdvec = new Vector<TDItem>(typeAr.length);
    	for (int i = 0; i < typeAr.length; i++) {
    		m_tdvec.add(new TDItem(typeAr[i],fieldAr[i]));
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	m_tdvec = new Vector<TDItem>(typeAr.length);
    	for (int i = 0; i < typeAr.length; i++) {
    		m_tdvec.add(new TDItem(typeAr[i],null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return m_tdvec.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i > numFields())
    		throw new NoSuchElementException();
    	return m_tdvec.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i > numFields())
    		throw new NoSuchElementException();
    	return m_tdvec.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // for (Iterator<TDItem> it = this.iterator(); it.hasNext(); it.next()){
    	if (name == null)
    		throw new NoSuchElementException("field name is null");
        
    	boolean allNull = true;
        for (int i = 0; i < m_tdvec.size(); i++){
        	String f = m_tdvec.get(i).fieldName;
        	if (f == null) continue;
        	allNull = false;
        	if (f.equals(name)){
        		return i;
        	}
        }    	
        if (allNull)
        	throw new NoSuchElementException("all field names null");

        throw new NoSuchElementException("name not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size = 0;
        for (TDItem tdi : m_tdvec){
        	size += tdi.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int newSize = td1.numFields() + td2.numFields();
    	Type[] newTypes = new Type[newSize];
    	String[] newNames = new String[newSize];
    	for (int i = 0; i < td1.numFields(); i++){
    		newTypes[i] = td1.getFieldType(i);
    		newNames[i] = td1.getFieldName(i);
    	}
    	for (int i = 0; i < td2.numFields(); i++) {
    		newTypes[i + td1.numFields()] = td2.getFieldType(i);
    		newNames[i + td1.numFields()] = td2.getFieldName(i);
    	}
        // some code goes here
        return new TupleDesc(newTypes, newNames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o == null){
    		return false;
    	}
    	if (!(o instanceof TupleDesc))
    		return false;
    	TupleDesc td = (TupleDesc) o;
    	if (this.getSize() == td.getSize() && this.numFields() == td.numFields()) {
    		for (int i = 0; i < this.numFields(); i++){
    			if (!this.getFieldType(i).equals(td.getFieldType(i)))
    				return false;
    		}
    		return true;
    	}
        // some code goes here
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuffer sb = new StringBuffer();
    	for (int i = 0; i < this.numFields(); i++) {
    		sb.append(m_tdvec.get(i).fieldType + "[" + i + "]" + "(" + m_tdvec.get(i).fieldName + "), ");
    	}
        return sb.toString();
    }
}
