package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUPING_KEY = new IntField(-1);
    private int gbField;
    private Type gbType;
    private int aggField;
    private Op aggOp;
    private TupleDesc td;
    private ConcurrentHashMap<Field, Integer> aggregator;       // (groupVal, aggregateVal)

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException("StringAggregator: COUNT Operator ONLY!");
        this.gbField = gbfield;
        this.gbType = gbfieldtype;
        this.aggField = afield;
        this.aggOp = what;      // could only be COUNT
        this.td = Aggregator.getTupleDesc(this.gbField, this.gbType);
        this.aggregator = new ConcurrentHashMap<>();
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (!tup.getTupleDesc().getFieldType(gbField).equals(this.gbType)) return;
        Field keyField;
        if (Aggregator.isNonGrouping(this.gbField)) keyField = null;
        else keyField = tup.getField(gbField);
        if (!aggregator.containsKey(keyField)) {
            aggregator.put(keyField, 1);
        } else {
            int current = aggregator.get(keyField);
            aggregator.put(keyField, current + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tupleList = new ArrayList<>();     //(groupVal, aggregateVal)
        for (Field eachGroup : aggregator.keySet()) {
            Tuple t = new Tuple(this.td);
            int groupResult = aggregator.get(eachGroup);
            if (Aggregator.isNonGrouping(this.gbField)) {
                t.setField(0, new IntField(groupResult));
            } else {
                t.setField(0, eachGroup);
                t.setField(1, new IntField(groupResult));
            }
            tupleList.add(t);
        }
        return new TupleIterator(td, tupleList);
    }
}
