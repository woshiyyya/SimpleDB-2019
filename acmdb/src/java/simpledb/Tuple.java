package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    private RecordId record_id;
    private TupleDesc tuple_desc = null;
    private ArrayList<Field> field_vals;
    private boolean on_page = false;

    public Tuple(TupleDesc td) {
        tuple_desc = td;
        field_vals = new ArrayList<>();
        for(int i = 0; i < td.numFields(); i++){
            field_vals.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tuple_desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return record_id;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.record_id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        field_vals.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return field_vals.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuffer strbuf = new StringBuffer();
        for(Field f: field_vals){
            strbuf.append(f.toString()).append("\t");
        }
        return strbuf.append("\n").toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return field_vals.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tuple_desc = td;
        // TODO: If we need to clear field values?
        field_vals = new ArrayList<>();
        for(int i = 0; i < td.numFields(); i++){
            field_vals.add(null);
        }
    }

    public boolean equals(Object o){
        if(o == null || !o.getClass().equals(this.getClass())){
            return false;
        }

        Tuple tp = ((Tuple) o);
        // TODO：是否需要check record id

        if(this.field_vals.size() != tp.field_vals.size()){
            return false;
        }

        for(int i = 0; i < field_vals.size(); i++){
            if(!field_vals.get(i).equals(tp.field_vals.get(i))){
                return false;
            }
        }
        return true;
    }
}
