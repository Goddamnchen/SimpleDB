package simpledb;

import java.io.Serializable;
import java.util.*;

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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;
    //tuple descriptor for each field in tuple
    private List<TDItem> tdItemList;

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
        this.tdItemList = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tdItemList.add(new TDItem(typeAr[i], fieldAr[i]));
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
        this.tdItemList = new ArrayList<>();
        for (Type each : typeAr) {
            tdItemList.add(new TDItem(each,""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        int fCount = tdItemList.size();
        return fCount;
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
        if (!validateIndex(i)) {
            throw new NoSuchElementException("Invalid index i in getFieldName(i)");
        }
        String fieldName = tdItemList.get(i).fieldName;
        if (fieldName == null) {
            return null;
        }
        return fieldName;
    }

    private boolean validateIndex(int i) {
        if (i < 0 || i >= tdItemList.size()) return false;
        else return true;
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
        if (!validateIndex(i)) {
            throw new NoSuchElementException();
        }
        Type fieldTpye = tdItemList.get(i).fieldType;
        return fieldTpye;
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
        for (int i = 0; i < this.numFields(); i++) {
            if (tdItemList.get(i).fieldName.equals(name))
                return i;
        }

        throw new NoSuchElementException("no matched name filed in tuple ");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes
        // TODO: Type size??
        int size = 0;
        for (TDItem each : tdItemList) {
            size += each.fieldType.getLen();
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
        // some code goes here
        int totalSize = td1.numFields() + td2.numFields();
        Type[] typeList = new Type[totalSize];
        String[] nameList = new String[totalSize];
        for (int i = 0; i < td1.numFields(); i++) {
            typeList[i] = td1.getFieldType(i);
            nameList[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); i++) {
            typeList[i + td1.numFields()] = td2.getFieldType(i);
            nameList[i + td1.numFields()] = td2.getFieldName(i);
        }
        TupleDesc newTD = new TupleDesc(typeList, nameList);
        return newTD;
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
        // some code goes here
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        TupleDesc td = (TupleDesc) o;
        if (td.numFields() != this.numFields()) {
            return false;
        }
        // equal size now
        int ith = 0;
        for (TDItem each : tdItemList) {
            if (each.fieldType != td.getFieldType(ith)) {
                return false;
            }
            ith++;

        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        // StringBuilder works faster and saves memory when concatenate lots of strings
        StringBuilder sb = new StringBuilder();
        for (TDItem each : tdItemList) {
            sb.append(", ");
            sb.append(each.toString());
        }
        return sb.toString();
    }
}
