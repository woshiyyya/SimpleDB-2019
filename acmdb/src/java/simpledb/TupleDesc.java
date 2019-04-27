package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    private ArrayList<TDItem> field_list;


    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
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
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return field_list.iterator();
    }

    private static final long serialVersionUID = 1L;

    public TupleDesc() {
        field_list = new ArrayList<>();
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length < 1) {
            throw new NoSuchElementException("must contain at least one entry");
        }

        if (typeAr.length != fieldAr.length) {
            throw new NoSuchElementException("Field names must not be NULL");
        }


        field_list = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            field_list.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr.length < 1) {
            throw new NoSuchElementException("must contain at least one entry");
        }

        field_list = new ArrayList<>();

        for (Type t : typeAr) {
            field_list.add(new TDItem(t, ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.field_list.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException("idx is not a valid field reference!");
        }
        return field_list.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException("idx is not a valid field reference!");
        }
        return field_list.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException("Field name must not be null!");
        }

        for (int i = 0; i < this.numFields(); i++) {
            if (name.equals(field_list.get(i).fieldName)) {
                return i;
            }
        }

        throw new NoSuchElementException("Field name not found!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int ByteSize = 0;
        for (TDItem td : field_list) {
            ByteSize += td.fieldType.getLen();
        }
        return ByteSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TupleDesc td = new TupleDesc();
        td.field_list.addAll(td1.field_list);
        td.field_list.addAll(td2.field_list);
        return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass()) || ((TupleDesc) o).getSize() != getSize()) {
            return false;
        }

        TupleDesc object = ((TupleDesc) o);

        for (int i = 0; i < this.numFields(); i++) {
            if (!object.getFieldType(i).equals(this.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // TODO: if two equal Arraylist has the same hash code?
        return field_list.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer strbuf = new StringBuffer();
        for (TDItem td : field_list) {
            strbuf.append(td.toString()).append(", ");
        }
        return strbuf.toString();
    }
}
