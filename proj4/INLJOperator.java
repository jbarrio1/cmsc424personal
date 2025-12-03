package edu.umd.cs424.database.query;

import edu.umd.cs424.database.Database;
import edu.umd.cs424.database.DatabaseException;
import edu.umd.cs424.database.databox.DataBox;
import edu.umd.cs424.database.table.Record;
import edu.umd.cs424.database.table.RecordIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Simple Nested Loop Join algorithm.
 */
public class INLJOperator extends JoinOperator {

    public INLJOperator(QueryOperator leftSource, QueryOperator rightSource, String leftColumnName, String rightColumnName, Database.Transaction transaction, String tableName) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.INLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new INLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        // ignore this
        return 0;
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Note that the left table is the "outer" loop and the right table is the
     * "inner" loop.
     */
    private class INLJIterator extends JoinIterator {
        // Iterator over all the records of the left relation.
        // Think of this like the outer table with a small size.
        private RecordIterator leftSourceIterator;

        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;


        public INLJIterator() throws QueryPlanException, DatabaseException {
            super();
            this.leftSourceIterator = INLJOperator.this.getRecordIterator(this.getLeftTableName());
            leftRecord = null;
        }

        /**
         * Returns the next record that should be yielded from this sjoin,
         * or null if there are no more records to join.
         * 
         * NOTE: To implement this, take a look at IndexScanOperator's constructor
         * on how to lookup a key on a table using the transaction.
         */
        private Record fetchNextRecord() throws DatabaseException {
            if (this.leftSourceIterator.hasNext()) {
                leftRecord = this.leftSourceIterator.next();
            } else {
                // there is no more records to compare from the left table
                return null;
            }
            // I'm thinking to get a iterator of records that equal the value of join column
            // through the index and iterator through return records.
            while(true) {
                DataBox joinValue = leftRecord.getValues().get(INLJOperator.this.getLeftColumnIndex());
                IndexScanOperator s;
                Iterator<Record> index_iterator = null;
                try {
                    s = new IndexScanOperator(INLJOperator.this.getTransaction(), this.getRightTableName(),
                            INLJOperator.this.getRightColumnName(), QueryPlan.PredicateOperator.EQUALS, joinValue);
                    index_iterator = s.iterator();
                } catch (QueryPlanException e) {
                    throw new RuntimeException(e);
                }
                // Since there can be at most one match
                if (index_iterator.hasNext()) {
                    return generateJoinRecord(leftRecord, index_iterator.next());
                } else if (leftSourceIterator.hasNext()) {
                    // ith record didn't have a match move onto the next one b/c there is one
                    leftRecord = leftSourceIterator.next();
                } else {
                    // no more left records to check
                    return null;
                }
            }

        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) {
                try {
                    this.nextRecord = fetchNextRecord();
                } catch (DatabaseException e) {
                    return false;
                }
            }
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
