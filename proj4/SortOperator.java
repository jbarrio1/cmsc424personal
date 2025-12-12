package edu.umd.cs424.database.query;

import edu.umd.cs424.database.Database;
import edu.umd.cs424.database.DatabaseException;
import edu.umd.cs424.database.table.Record;
import edu.umd.cs424.database.table.Schema;
import edu.umd.cs424.database.common.Pair;

import java.util.*;

public class SortOperator {
    private Database.Transaction transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(Database.Transaction transaction, String tableName,
                        Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getNumMemoryPages();
    }

    public Schema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * Return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Run run) throws DatabaseException {
         List<Record> needSort = new ArrayList<>();
         var it = run.iterator();

         while (it.hasNext()) {
             needSort.add(it.next());
         }
         needSort.sort(this.comparator);

         Run sortedRun = createRun();
         sortedRun.addRecords(needSort);

        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * Return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
        PriorityQueue<Pair<Record,Integer>> pq = new PriorityQueue<>(new RecordPairComparator());
        List<Iterator<Record>> iterators = new ArrayList<>();

        // getting iterators for all the runs
        for ( Run run : runs ){
            iterators.add(run.iterator());
        }
        // adding the first element of each list into priority queue
        for (int i = 0 ; i < iterators.size() ; i++ ) {

            //kind of dangerous if one of the runs is empty, I'm assuming they wont be empty
            pq.add(new Pair(iterators.get(i).next(), i));
        }

        List<Record> mergedRuns = new ArrayList<>();
        while(!pq.isEmpty()){
            // add smallest element - assuming sorting is ascending order
            var currSmallest = pq.poll();
            mergedRuns.add(currSmallest.getFirst());

            // add next element from run or skip if empty I have to check here
            if (iterators.get(currSmallest.getSecond()).hasNext()) {
                pq.add(new Pair (iterators.get(currSmallest.getSecond()).next(), currSmallest.getSecond()));
            }
        }


        Run finalRun = createRun();
        finalRun.addRecords(mergedRuns);

        return finalRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * Return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) throws DatabaseException {
        //I guess the first step is split the runs up example, lets say num buffers are 3 and runs.size() = 8
        // I am going to return a list of (0,1) merged, (2,3) merged, (4,5) merged(6,7),
        // lets say number buffers = 4 (3)  , (0,1,2) m , (3,4,5) m , (6,7) m runs.size() =8

        // i = 2, whe
        List<Run> merged = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < runs.size(); i++){
            var div = i % (this.numBuffers -1) ;
            // we would be moving more than numbuffers -1 at this point
            if ( div == 0 && i != 0 ) {
                merged.add(mergeSortedRuns(runs.subList(start, i)));
                start = i;
            }
        }
        // there is only one run left
        if (start == runs.size() -1 ) {
            merged.add(runs.get(start));
        } else {
            merged.add(mergeSortedRuns(runs.subList(start,runs.size())));
        }

        return merged;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     * Some key implementation details:
     * 1. If the table is empty, throw a DatabaseException.
     * 2. Use a block iterator to read numBuffers - 1 data pages and create a run.
     *    Remember to skip the header page when necessary.
     * 3. Sort each newly created run.
     * 4. Merge the sorted runs.
     */
    public String sort() throws DatabaseException {
        // find out if the table is empty
        var pageIt = transaction.getPageIterator(this.tableName);
        var recordIt = transaction.getBlockIterator(this.tableName, pageIt);
        if (!recordIt.hasNext()){
            throw new DatabaseException("This relation was empty (didn't return any records");
        }
        // createRuns which is basically numbuf -1 pages -> sort
        int marker = 0;
        List<Record> tempRun = new ArrayList<>();
        List<Run> sortedRuns = new ArrayList<>();

        while (pageIt.hasNext()){
            var div = marker % (this.numBuffers -1 );

            // run is ready to be created
            if (div ==0 &&  marker !=0  ) {
                var runReady = createRun();
                runReady.addRecords(tempRun);
                sortedRuns.add(sortRun(runReady));
                // reset tempRun
                tempRun.clear();
                marker++;

            } else {
                // add the records to the current run
                var pageRecords = this.transaction.getBlockIterator(this.tableName,pageIt,1);
                while (pageRecords.hasNext()){
                    tempRun.add(pageRecords.next());
                }
                marker++;
            }
        }
        if (!tempRun.isEmpty()){
            var runReady = createRun();
            runReady.addRecords(tempRun);
            sortedRuns.add(sortRun(runReady));
        }

        // check if merging  can be done in one pass && seems like this the assumption

        var singleRun = mergePass(sortedRuns);
        var finalRun = singleRun.get(0);
        return finalRun.tableName();
    }

    public Iterator<Record> iterator() throws DatabaseException {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

        }
    }

    public Run createRun() throws DatabaseException {
        return new Run(this.transaction, this.operatorSchema);
    }
}

