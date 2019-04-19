package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.NaturalJoinPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;
import edu.brandeis.shangyuzhang.util.Database;

import java.io.IOException;
import java.util.*;

public abstract class BaseJoin {

    protected int numRows;

    protected Iterator leftTable;
    protected Iterator rightTable;

    protected String firstTableName;
    protected FilterPredicate firstTableFilterPredicate;

    protected String currTableName;
    protected FilterPredicate currTableFilterPredicate;

    protected List<ParseElem[]> naturalJoinPairs;
    protected Map<String, Integer> tableStartIndexMap;

    protected boolean isCartesianJoin;

    protected static final int BUFFER_SIZE = 1024 * 32;
    protected Database database = Database.getInstance();

    public BaseJoin(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startIndexMap,
                    List<ParseElem[]> pairs, String firstTable, FilterPredicate firstFilterPred, String currTable, FilterPredicate currFilterPred) {
        leftTable = leftIterator;
        rightTable = rightIterator;

        tableStartIndexMap = startIndexMap;

        firstTableName = firstTable;
        firstTableFilterPredicate = firstFilterPred;

        currTableName = currTable;
        currTableFilterPredicate = currFilterPred;

        naturalJoinPairs = pairs;
        isCartesianJoin = pairs.isEmpty();
    }

    public abstract boolean isEmptyTable();

    public abstract void resetIterator() throws IOException;

    protected int translateColByTableName(String tableName, int col) {
        return tableStartIndexMap.get(tableName) + database.getRelationByName(tableName).getNewByOldCol(col);
    }

    protected abstract void toJoinTable() throws IOException;

    protected void cartesianJoin() throws IOException {
        int[][] bufferRows = new int[BUFFER_SIZE][];
        int pointer = 0;
        while (true) {
            boolean isDiskEnd = true;
            while (rightTable.hasNext()) {
                bufferRows[pointer++] = (int[]) rightTable.next();
                if (pointer < BUFFER_SIZE) continue;
                isDiskEnd = false;
                break;
            }
            while (leftTable.hasNext()) {
                int[] memRow = (int[]) leftTable.next();
                for (int i = 0; i < pointer; i++) addToResultRows(memRow, bufferRows[i]);
            }
            pointer = 0;
            if (isDiskEnd) break;
            resetLeftIterator();
        }
    }

    protected void naturalJoin() throws IOException {
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
            while (rightTable.hasNext()) {
                bufferRows[pointer] = (int[]) rightTable.next();
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

            while (leftTable.hasNext()) {
                int[] memRow = (int[]) leftTable.next();

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
            resetLeftIterator();
        }
    }

    public int getNumRows() {
        return numRows;
    }

    protected abstract void addToResultRows(int[] memRow, int[] diskRow);

    private void resetLeftIterator() throws IOException {
        if (leftTable instanceof BaseJoin) {
            ((BaseJoin) leftTable).resetIterator();
        } else if (leftTable instanceof Filter) {
            leftTable = null;
            leftTable = new Filter(new Project(new Scan(firstTableName), database.getRelationByName(firstTableName).getColsToKeep()), firstTableFilterPredicate);
        } else if (leftTable instanceof Project) {
            leftTable = null;
            leftTable = new Project(new Scan(firstTableName), database.getRelationByName(firstTableName).getColsToKeep());
        } else if (leftTable instanceof Scan) {
            leftTable = null;
            leftTable = new Scan(firstTableName);
        }
    }

    private void resetRightIterator() throws IOException {
        if (rightTable instanceof Filter) {
            rightTable = null;
            rightTable = new Filter(new Project(new Scan(currTableName), database.getRelationByName(currTableName).getColsToKeep()), currTableFilterPredicate);
        } else if (rightTable instanceof Project) {
            rightTable = null;
            rightTable = new Project(new Scan(currTableName), database.getRelationByName(currTableName).getColsToKeep());
        } else if (rightTable instanceof Scan) {
            rightTable = null;
            rightTable = new Scan(currTableName);
        }
    }

}
