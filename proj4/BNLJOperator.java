package edu.umd.cs424.database.query;

import java.util.*;

import edu.umd.cs424.database.Database;
import edu.umd.cs424.database.DatabaseException;
import edu.umd.cs424.database.common.BacktrackingIterator;
import edu.umd.cs424.database.databox.DataBox;
import edu.umd.cs424.database.io.PageAllocator.PageIterator;
import edu.umd.cs424.database.table.Record;

public class BNLJOperator extends JoinOperator {
    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction,
              JoinType.BNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new BNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        //does nothing
        return 0;
    }

    /**
     * Block Nested Loop Join
     * See Section 15.5.2 in the textbook.
     *
     * A record iterator that executes the logic for a block nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over all the pages of the left source
        private PageIterator leftSourceIterator;
        // Iterator over all the pages of the right source
        private PageIterator rightSourceIterator;
        // Iterator over records in the current left page
        private BacktrackingIterator<Record> leftRecordPerPageIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordPerPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() throws QueryPlanException, DatabaseException {
            super();
            this.leftSourceIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            this.leftSourceIterator.next();
            this.leftRecordPerPageIterator = null;
            this.leftRecord = null;
            this.fetchNextLeftPage();

            this.rightSourceIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightSourceIterator.next();
            this.rightRecordPerPageIterator = null;
            if (this.rightSourceIterator.hasNext()) {
                this.rightSourceIterator.next();
                this.rightSourceIterator.mark();
                this.rightSourceIterator.reset();
            }
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left table.
         * leftRecordPerPageIterator should be set to a backtracking iterator over 1 page
         * of records from the left table, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more pages in the left table, this method should
         * do nothing.
         *
         * You may find BNLJOperator.this.getBlockIterator useful here. Note that there
         * are multiple getBlockIterator implementations, and you should use the one
         * that allows you to manually specify the number of pages per block, which is 1
         * for BNLJ.
         */
        private void fetchNextLeftPage() throws DatabaseException {

            // first check if there is any more pages to fetch
            if (leftSourceIterator.hasNext()) {

                //if so fetch the records from that page
                this.leftRecordPerPageIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(),
                        this.leftSourceIterator,1);

                //assign the first record of that page
                this.leftRecord = leftRecordPerPageIterator.next();
                this.leftRecordPerPageIterator.mark();

            }
        }



        /**
         * Fetch the next page of records from the right table.
         * rightRecordPerPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right table, and is marked such that the next
         * time reset() is called, a subsequent next() call will return the first record
         * in this page.
         *
         * If there are no more pages in the right table, this method should
         * do nothing.
         *
         * You may find BNLJOperator.this.getBlockIterator useful here. Note that there
         * are multiple getBlockIterator implementations, and you should use the one
         * that allows you to manually specify the number of pages per block, which is 1
         * for BNLJ.
         */
        private void fetchNextRightPage() throws DatabaseException {
            // first check if there is any more pages to fetch
            if (rightSourceIterator.hasNext()) {

                //if so fetch the next page and set backtracking marking
                this.rightRecordPerPageIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(),
                        this.rightSourceIterator,1);

                this.rightRecordPerPageIterator.next();
                this.rightRecordPerPageIterator.mark();
                this.rightRecordPerPageIterator.reset();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() throws DatabaseException {
            if (this.leftRecord == null){
                //Somehow the page we fetched had no records
                return null;
            }

            while(true){
                if (this.rightRecordPerPageIterator.hasNext()) {
                    Record rightRecord = this.rightRecordPerPageIterator.next();

                    DataBox leftJoinValue = leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        return generateJoinRecord(leftRecord, rightRecord);
                    }
                } else if (leftRecordPerPageIterator.hasNext()){
                        // I'm out of right records on current page to compare
                        // advance the current left record and compare again with current right page
                        this.leftRecord = leftRecordPerPageIterator.next();
                        this.rightRecordPerPageIterator.reset();
                } else if(rightSourceIterator.hasNext()){
                        // we need the next left page if one exists
                        fetchNextRightPage();
                        leftRecordPerPageIterator.reset();
                        leftRecord =  leftRecordPerPageIterator.next();

                } else if (leftSourceIterator.hasNext()) {

                        //went through all the pages in R now I need to advance left page
                        fetchNextLeftPage();
                        rightSourceIterator.reset();
                        fetchNextRightPage();
                } else {
                        return null;
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            try {
                if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
                return this.nextRecord != null;
            } catch (DatabaseException e) {
                return false;
            }
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}

