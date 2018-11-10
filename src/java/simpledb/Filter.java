package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate pred;
    private DbIterator child;
    private Tuple current;      // seems redundant, no need to maintain this state

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.pred = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        Predicate predicate = this.pred;
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = child.getTupleDesc();
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.current = null;
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        current = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        // On-the-fly iterator, streaming, small and constant work per call
        while (child.hasNext()) {
            current = child.next();
            if (pred.filter(current)) return current;
        }
        return null;
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
