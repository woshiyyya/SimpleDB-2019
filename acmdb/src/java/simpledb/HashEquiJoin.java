package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate joinPredicate;
    private DbIterator child1;
    private DbIterator child2;
    private HashMap<Field, ArrayList<Tuple>> hashMap = new HashMap<>();
    private int field_id1;
    private int field_id2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        field_id1 = p.getField1();
        field_id2 = p.getField2();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        hashMap.clear();
        child1.open();
        child2.open();

        while (child1.hasNext()) {
            Tuple tuple = child1.next();
            Field field = tuple.getField(field_id1);
            if (!hashMap.containsKey(field)) {
                hashMap.put(field, new ArrayList<>());
            }
            hashMap.get(field).add(tuple);
        }
        child1.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child1.close();
        this.child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child1.rewind();
        this.child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Tuple right_tuple = null;

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (true) {
            // Find next valid right tuple
            while (right_tuple == null) {
                if (!child2.hasNext()) {
                    return null;
                }
                right_tuple = child2.next();

                Field f = right_tuple.getField(field_id2);
                if (hashMap.containsKey(f)) {
                    listIt = hashMap.get(f).iterator();
                } else {
                    right_tuple = null;
                }
            }

            // Find valid left tuple
            if (listIt.hasNext()) {
                return Tuple.merge(listIt.next(), right_tuple, getTupleDesc());
            } else {
                right_tuple = null;
            }
        }
    }


    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }

}
