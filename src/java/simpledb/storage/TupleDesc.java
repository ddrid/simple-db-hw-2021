package simpledb.storage;

import simpledb.common.Type;

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
        return tupleDescList.iterator();
    }

    private static final long serialVersionUID = 1L;
    private final List<TDItem> tupleDescList;

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
        if (typeAr.length != fieldAr.length)
            throw new IllegalArgumentException("Length of typeAr and fieldAr are not equal");
        tupleDescList = new ArrayList<>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            tupleDescList.add(new TDItem(typeAr[i], fieldAr[i]));
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
        tupleDescList = new ArrayList<>(typeAr.length);
        for (Type type : typeAr) {
            tupleDescList.add(new TDItem(type, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tupleDescList.size();
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
        if (i > numFields() - 1) throw new NoSuchElementException();
        return tupleDescList.get(i).fieldName;
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
        if (i > numFields() - 1) throw new NoSuchElementException();
        return tupleDescList.get(i).fieldType;
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
        for (int i = 0; i < tupleDescList.size(); i++) {
            String fieldName = tupleDescList.get(i).fieldName;
            if (fieldName != null && fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem item : tupleDescList) {
            size += item.fieldType.getLen();
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
        int td1Len = td1.numFields();
        int td2Len = td2.numFields();
        int len = td1Len + td2Len;
        Type[] typeAr = new Type[len];
        String[] fieldAr = new String[len];
        Iterator<TDItem> it1 = td1.iterator();
        int index = 0;
        while (it1.hasNext()) {
            TDItem item = it1.next();
            typeAr[index] = item.fieldType;
            fieldAr[index++] = item.fieldName;
        }
        Iterator<TDItem> it2 = td2.iterator();
        while (it2.hasNext()) {
            TDItem item = it2.next();
            typeAr[index] = item.fieldType;
            fieldAr[index++] = item.fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc) || ((TupleDesc) o).numFields() != numFields()) {
            return false;
        } else {
            Iterator<TDItem> it = ((TupleDesc) o).iterator();
            int index = 0;
            while (it.hasNext()) {
                TDItem item = it.next();
                if (!item.fieldType.equals(getFieldType(index)))
                    return false;
                String name = getFieldName(index);
                if (name == null) {
                    if (item.fieldName != null) return false;
                } else {
                    if (item.fieldName == null) return false;
                    if (!item.fieldName.equals(name)) return false;
                }
                index++;
            }
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
        StringBuilder sb = new StringBuilder();
        Iterator<TDItem> it = iterator();
        while (it.hasNext()) {
            TDItem item = it.next();
            sb.append(item.fieldType.name())
                    .append("(")
                    .append(item.fieldName)
                    .append(") ");
        }
        return sb.toString();
    }
}
