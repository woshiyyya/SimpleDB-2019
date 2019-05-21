package simpledb;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int afield;
    private int gbfield;
    private Op what;
    private TupleDesc AggregatedTupleDesc;
    private ArrayList<Tuple> AggregatedTuples = new ArrayList<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }

        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;

        if (gbfield == NO_GROUPING) {
            this.AggregatedTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            this.AggregatedTupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

    }

    private Field getfield(Tuple tup, int field_id) {
        if (field_id == NO_GROUPING) {
            return new IntField(-1);
        } else {
            return tup.getField(field_id);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Iterator<Tuple> iterator = AggregatedTuples.iterator();

        while (iterator.hasNext()) {
            Tuple inner_tuple = iterator.next();
            if (gbfield == NO_GROUPING) {
                int cnt = ((IntField) inner_tuple.getField(0)).getValue() + 1;
                IntField new_field = new IntField(cnt);
                inner_tuple.setField(0, new_field);
                return;
            } else {
                Field inner_groupby_field = inner_tuple.getField(0);
                Field outer_groupby_field = tup.getField(this.gbfield);
                if (inner_groupby_field.equals(outer_groupby_field)) {
                    int cnt = ((IntField) inner_tuple.getField(1)).getValue() + 1;
                    IntField new_field = new IntField(cnt);
                    inner_tuple.setField(1, new_field);
                    return;
                }
            }
        }


        Tuple initial_inner_tuple = new Tuple(AggregatedTupleDesc);
        Field initial_group_field = getfield(tup, gbfield);
        Field initial_aggregate_field = new IntField(1);

        if (gbfield == NO_GROUPING) {
            initial_inner_tuple.setField(0, initial_aggregate_field);
        } else {
            initial_inner_tuple.setField(0, initial_group_field);
            initial_inner_tuple.setField(1, initial_aggregate_field);
        }
        AggregatedTuples.add(initial_inner_tuple);

    }


    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(AggregatedTupleDesc, AggregatedTuples);
    }

}
