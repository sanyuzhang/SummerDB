package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.model.*;
import edu.brandeis.shangyuzhang.util.Database;

import java.io.IOException;
import java.util.*;

public class Join implements Iterator<int[]> {

    private int currRow;
    private List<int[]> rows;

    private Iterator diskTable;
    private Iterator memTable;

    private String rightTableName;
    private FilterPredicate rightTableFilterPredicate;

    private List<ParseElem[]> naturalJoinPairs;
    private Map<String, Integer> tableToStartColMap;

    private boolean isCartesianJoin;

    private List<ParseElem> sumElems;
    private long[] sums;
    private int[] sumCols;

    private static final int BUFFER_SIZE = 1024 * 8;
    private Database database = Database.getInstance();

    public Join(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                String rightTb, List<ParseElem[]> pairs, FilterPredicate rightFp) throws IOException {
        currRow = 0;

        memTable = leftIterator;
        diskTable = rightIterator;

        tableToStartColMap = startColMap;

        rightTableName = rightTb;
        rightTableFilterPredicate = rightFp;

        naturalJoinPairs = pairs;
        isCartesianJoin = pairs.isEmpty();


        rows = new ArrayList();
        toJoinTable();
    }

    public Join(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                String rightTb, List<ParseElem[]> pairs, FilterPredicate rightFp, List<ParseElem> sumElms) throws IOException {
        currRow = 0;

        memTable = leftIterator;
        diskTable = rightIterator;

        tableToStartColMap = startColMap;

        rightTableName = rightTb;
        rightTableFilterPredicate = rightFp;

        naturalJoinPairs = pairs;
        isCartesianJoin = pairs.isEmpty();

        sumElems = sumElms;
        initSumTools();

        rows = new ArrayList();
        toJoinTable();
    }

    private void initSumTools() {
        sums = new long[sumElems.size()];
        sumCols = new int[sumElems.size()];
        for (int i = 0; i < sumCols.length; i++) {
            ParseElem elem = sumElems.get(i);
            sumCols[i] = translateColByTableName(elem.table, elem.col);
        }
    }

    private void toJoinTable() throws IOException {
        if (isCartesianJoin) cartesianJoin();
        else naturalJoin();
    }

    private int translateColByTableName(String tableName, int col) {
        return tableToStartColMap.get(tableName) + database.getRelationByName(tableName).getNewByOldCol(col);
    }

    private void cartesianJoin() throws IOException {
        int[][] bufferRows = new int[BUFFER_SIZE][];
        int pointer = 0;
        while (true) {
            boolean isDiskEnd = true;
            while (diskTable.hasNext()) {
                bufferRows[pointer++] = (int[]) diskTable.next();
                if (pointer < BUFFER_SIZE) continue;
                isDiskEnd = false;
                break;
            }
            while (memTable.hasNext()) {
                int[] memRow = (int[]) memTable.next();
                for (int i = 0; i < pointer; i++) addToResultRows(memRow, bufferRows[i]);
            }
            pointer = 0;
            if (isDiskEnd) break;
            resetRightIterator();
        }
    }

    private void naturalJoin() throws IOException {
        List<Integer> newColsOnDisk = new ArrayList();
        NaturalJoinPredicate[] naturalJoinPredicates = new NaturalJoinPredicate[naturalJoinPairs.size()];
        for (int i = 0; i < naturalJoinPairs.size(); i++) {
            ParseElem leftElem = naturalJoinPairs.get(i)[0], rightElem = naturalJoinPairs.get(i)[1];
            int memCol = translateColByTableName(leftElem.table, leftElem.col);
            int diskCol = database.getRelationByName(rightElem.table).getNewByOldCol(rightElem.col);
            naturalJoinPredicates[i] = new NaturalJoinPredicate(diskCol, memCol);
            newColsOnDisk.add(diskCol);
        }

        int[][] bufferRows = new int[BUFFER_SIZE][];
        Map<Integer, Map<Integer, Set<Integer>>> bufferHash = new HashMap(); // k: col, v: map:{ k: colValue, v: list of row_numbers}
        int pointer = 0;
        while (true) {
            boolean isDiskEnd = true;
            while (diskTable.hasNext()) {
                bufferRows[pointer] = (int[]) diskTable.next();
                for (int i = 0; i < newColsOnDisk.size(); i++) {
                    int diskCol = newColsOnDisk.get(i);
                    int disColValue = bufferRows[pointer][diskCol];
                    bufferHash.putIfAbsent(diskCol, new HashMap());
                    bufferHash.get(diskCol).putIfAbsent(disColValue, new HashSet());
                    bufferHash.get(diskCol).get(disColValue).add(pointer);
                }
                pointer++;
                if (pointer < BUFFER_SIZE) continue;
                isDiskEnd = false;
                break;
            }

            while (memTable.hasNext()) {
                int[] memRow = (int[]) memTable.next();
                Set<Integer> rowsInBuffer = new HashSet();
                for (int i = 0; i < naturalJoinPredicates.length; i++) {
                    if (i > 0 && rowsInBuffer.size() == 0) break;
                    NaturalJoinPredicate predicate = naturalJoinPredicates[i];
                    int diskCol = predicate.col1, targetValue = memRow[predicate.col2];
                    if (bufferHash.containsKey(diskCol) && bufferHash.get(diskCol).containsKey(targetValue)) {
                        if (i == 0) rowsInBuffer = new HashSet(bufferHash.get(diskCol).get(targetValue));
                        else rowsInBuffer.retainAll(bufferHash.get(diskCol).get(targetValue));
                    } else if (i > 0) {
                        rowsInBuffer.clear();
                    }
                }
                for (int rowNum : rowsInBuffer) addToResultRows(memRow, bufferRows[rowNum]);
            }
            bufferHash.clear();
            pointer = 0;
            if (isDiskEnd) break;
            resetRightIterator();
        }
    }

    private void addToResultRows(int[] memRow, int[] diskRow) {
        if (sumElems != null) {
            for (int i = 0; i < sumCols.length; i++) {
                if (sumCols[i] < memRow.length) sums[i] += memRow[sumCols[i]];
                else sums[i] += diskRow[sumCols[i] - memRow.length];
            }
        } else {
            int[] newRow = mergeRow(memRow, diskRow);
            rows.add(newRow);
        }
    }

    private int[] mergeRow(int[] memRow, int[] diskRow) {
        int[] row = new int[memRow.length + diskRow.length];
        for (int i = 0; i < row.length; i++) {
            if (i < memRow.length) row[i] = memRow[i];
            else row[i] = diskRow[i - memRow.length];
        }
        return row;
    }

    public long[] getSums() {
        return sums;
    }

    public void setCurrRow(int currRow) {
        this.currRow = currRow;
    }

    private void resetRightIterator() throws IOException {
        if (memTable instanceof Scan) {
            memTable = null;
            memTable = new Scan(rightTableName);
        } else if (memTable instanceof Project) {
            memTable = null;
            memTable = new Project(new Scan(rightTableName), database.getRelationByName(rightTableName).getColsToKeep());
        } else if (memTable instanceof Filter) {
            memTable = null;
            memTable = new Filter(new Project(new Scan(rightTableName), database.getRelationByName(rightTableName).getColsToKeep()), rightTableFilterPredicate);
        } else if (memTable instanceof Join) {
            ((Join) memTable).setCurrRow(0);
        }
    }

    @Override
    public boolean hasNext() {
        if (currRow < rows.size()) {
            return true;
        }
        return false;
    }

    @Override
    public int[] next() {
        return rows.get(currRow++);
    }

}
