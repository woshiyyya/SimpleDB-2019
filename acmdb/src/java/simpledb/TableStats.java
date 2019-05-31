package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    private int IOCostPerPage;
    private int numPages;
    private int numTuples;
    private TupleDesc tupleDesc;
    private HashMap<String, Integer> MinVal;
    private HashMap<String, Integer> MaxVal;
    private HashMap<String, Object> Histograms;

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
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
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
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
        this.IOCostPerPage = ioCostPerPage;
        this.MinVal = new HashMap<>();
        this.MaxVal = new HashMap<>();
        this.Histograms = new HashMap<>();

        DbFile table = Database.getCatalog().getDatabaseFile(tableid);
        this.numPages = ((HeapFile) table).numPages();
        this.tupleDesc = table.getTupleDesc();
        int numFields = tupleDesc.numFields();

        SeqScan scan = new SeqScan(new TransactionId(), tableid);

        for (int i = 0; i < numFields; i++) {
            MinVal.put(tupleDesc.getFieldName(i), Integer.MAX_VALUE);
            MaxVal.put(tupleDesc.getFieldName(i), Integer.MIN_VALUE);
        }

        try {
            // First Scan calculate Min-Max Val for each Field
            scan.open();
            while (scan.hasNext()) {
                Tuple tuple = scan.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = tuple.getField(i);
                    String name = tupleDesc.getFieldName(i);
                    if (field.getType() == Type.INT_TYPE) {
                        int val = ((IntField) field).getValue();
                        if (val > MaxVal.get(name))
                            MaxVal.put(name, val);
                        if (val < MinVal.get(name))
                            MinVal.put(name, val);
                    }
                }
            }

            // Initialize Histograms for each Field
            for (int i = 0; i < numFields; i++) {
                String name = tupleDesc.getFieldName(i);
                if (tupleDesc.getFieldType(i) == Type.INT_TYPE)
                    Histograms.put(name, new IntHistogram(NUM_HIST_BINS, MinVal.get(name), MaxVal.get(name)));
                if (tupleDesc.getFieldType(i) == Type.STRING_TYPE)
                    Histograms.put(name, new StringHistogram(NUM_HIST_BINS));
            }

            // Second Scan to build up Histograms
            scan.rewind();
            while (scan.hasNext()) {
                numTuples++;
                Tuple tuple = scan.next();
                for (int i = 0; i < numFields; i++) {
                    String name = tupleDesc.getFieldName(i);
                    Type fieldType = tupleDesc.getFieldType(i);
                    if (fieldType == Type.INT_TYPE) {
                        int val = ((IntField) tuple.getField(i)).getValue();
                        ((IntHistogram) Histograms.get(name)).addValue(val);
                    }
                    if (fieldType == Type.STRING_TYPE) {
                        String val = ((StringField) tuple.getField(i)).getValue();
                        ((StringHistogram) Histograms.get(name)).addValue(val);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return IOCostPerPage * numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = tupleDesc.getFieldType(field);
        String name = tupleDesc.getFieldName(field);
        if (fieldType == Type.INT_TYPE){
            int val = ((IntField) constant).getValue();
            return ((IntHistogram) Histograms.get(name)).estimateSelectivity(op, val);
        } else{
            String val = ((StringField) constant).getValue();
            return ((StringHistogram) Histograms.get(name)).estimateSelectivity(op, val);
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

}
