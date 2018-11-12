package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUPING_KEY = new IntField(-1);
    private int gbField;
    private Type gbType;
    private int aggField;
    private Op aggOp;
    private TupleDesc td;       // used to create tuple iterator
    private ConcurrentHashMap<Field, Integer> aggregator;       // (groupVal, aggregateVal)
    private ConcurrentHashMap<Field, Integer> counter;          // (groupVal, count)
    private ConcurrentHashMap<Field, Integer> summor;           // (groupVal, sum)

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbType = gbfieldtype;
        this.aggField = afield;
        this.aggOp = what;
        this.td = Aggregator.getTupleDesc(this.gbField, this.gbType);
        aggregator = new ConcurrentHashMap<>();
        summor = new ConcurrentHashMap<>();
        counter = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // obtain key grouping by
        Field keyField;
        if (Aggregator.isNonGrouping(this.gbField)) keyField = NO_GROUPING_KEY;
        else keyField = tup.getField(gbField);

        // get value(to be aggregated) of current tuple tup
        // initialize if not exits a current group of tuples
        int readIn = ((IntField) (tup.getField(this.aggField))).getValue();
        if (!aggregator.containsKey(keyField)) {
            aggregator.put(keyField, initGroup(keyField, readIn));
        } else {
            int current = aggregator.get(keyField);
            aggregator.put(keyField, aggregate(keyField, current, readIn));
        }
    }

    /**
     * Helper method used for initiation of first-time-appeared group key
     * @param key first-time-appeared group key
     * @param readIn first-time-appeared value corresponding to that group key
     * @return first-time aggregate result
     */
    private int initGroup(Field key, int readIn) {
        int initialValue;
        switch(aggOp) {
            case MIN: case MAX: case SUM: case AVG:
                initialValue = readIn;
                break;
            case COUNT:
                initialValue = 1;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized IntegerAggregator Operator during initGroup");
        }
        summor.put(key, readIn);
        counter.put(key, 1);
        return initialValue;
    }

    /**
     * Helper method used for aggregation of group key
     * @param key group key of a new tuple
     * @param current previous aggregate result of this group key
     * @param readIn value corresponding to the new group key
     * @return aggregate result based on aggregation operation
     */
    private int aggregate(Field key, int current, int readIn) {
        int resultValue = current;
        int sum = summor.get(key) + readIn;
        int count = counter.get(key) + 1;
        switch (aggOp) {
            case MIN:
                if (readIn < current) resultValue = readIn;
                break;
            case MAX:
                if (readIn > current) resultValue = readIn;
                break;
            case SUM:
                resultValue = current + readIn;
                break;
            case COUNT:
                resultValue = current + 1;
                break;
            case AVG:
                resultValue = sum / count;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized IntegerAggregator Operator during aggregate");
        }
        aggregator.put(key, resultValue);
        counter.put(key, count);
        summor.put(key, sum);
        return resultValue;
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tupleList = new ArrayList<>();     //(groupVal, aggregateVal)
        for (Field eachGroup : aggregator.keySet()) {
            Tuple t = new Tuple(this.td);
            int groupResult = aggregator.get(eachGroup);
            if (Aggregator.isNonGrouping(this.gbField)) t.setField(0, new IntField(groupResult));
            else {
                t.setField(0, eachGroup);
                t.setField(1, new IntField(groupResult));
            }
            tupleList.add(t);
        }
        return new TupleIterator(td, tupleList);
    }

}
