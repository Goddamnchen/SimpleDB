package simpledb;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int aggColumn;
    private int gbColumn;
    private Aggregator.Op aggOp;
    private DbIterator aggIt;       // Iterator of aggregated results generated by InterAggregator/StringAggregator

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        this.child = child;
        this.aggColumn = afield;
        this.gbColumn = gfield;
        this.aggOp = aop;
        this.aggIt = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // some code goes here
        int gbColum = this.gbColumn;
        return gbColum;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    // some code goes here
        if (this.gbColumn == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gbColumn);
    }
    /**
     * Helper method
     * @return If this aggregate is accompanied by a group by, return the type
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    private Type groupFieldType() {
        if (this.gbColumn == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldType(gbColumn);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    // some code goes here
        int aggColum = this.aggColumn;
        return aggColum;
    }

    /**
     * @return the aggregate field
     * */
    public Type aggregateFieldType() {
        // some code goes here
        return child.getTupleDesc().getFieldType(this.aggColumn);
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    // some code goes here
	    return child.getTupleDesc().getFieldName(aggColumn);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    // some code goes here
        Aggregator.Op aop = this.aggOp;
    	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        Aggregator aggregator = null;
        switch(aggregateFieldType()){
            case INT_TYPE :
                aggregator = new IntegerAggregator(gbColumn, groupFieldType(), aggColumn, aggOp);
                break;
            case STRING_TYPE :
                aggregator = new StringAggregator(gbColumn, groupFieldType(), aggColumn, aggOp);
                break;
            default :
                throw new DbException("Non-existed aggregate field type:" + aggregateFieldType());
        }
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();

        this.aggIt = aggregator.iterator();
        aggIt.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here
        Tuple aggTuple;
        if (aggIt.hasNext())
            aggTuple = aggIt.next();
        else {
            aggTuple = null;
        }
	    return aggTuple;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        this.close();
        this.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
        String aggColumName = aggregateFieldName() + "(" + nameOfAggregatorOp(this.aggOp) + ")" +
                              " (" + groupFieldName() + ")" ;
        Type[] fieldTypes;
        String[] fieldNames;
        if (this.gbColumn == Aggregator.NO_GROUPING) {
            fieldTypes = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{aggColumName};
        } else {
            fieldTypes = new Type[]{groupFieldType(), Type.INT_TYPE};
            fieldNames = new String[]{groupFieldName(), aggColumName};
        }
        return new TupleDesc(fieldTypes, fieldNames);
    }

    public void close() {
	    // some code goes here
        super.close();
        aggIt.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    // some code goes here
        this.child = children[0];
    }
    
}
