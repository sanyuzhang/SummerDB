package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.interfaces.RowsCounter;
import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.NaturalJoinPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;
import edu.brandeis.shangyuzhang.util.Database;

import java.io.IOException;
import java.util.*;

public abstract class BaseJoin implements RowsCounter {

    protected int numRows;

    protected Iterator leftTable;
    protected Iterator rightTable;

    protected String firstTableName;
    protected FilterPredicate firstTableFilterPredicate;
    protected int firstTableNumOfRows;

    protected String currTableName;
    protected FilterPredicate currTableFilterPredicate;
    protected int currTableNumOfRows;

    protected List<ParseElem[]> naturalJoinPairs;
    protected Map<String, Integer> tableStartIndexMap;

    protected boolean isCartesianJoin;

    protected static final int BUFFER_SIZE = 1024 * 32;
    protected Database database = Database.getInstance();

    public BaseJoin(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startIndexMap,
                    List<ParseElem[]> pairs, String firstTable, FilterPredicate firstFilterPred, int firstNumOfRows,
                    String currTable, FilterPredicate currFilterPred, int currNumOfRows) {
        leftTable = leftIterator;
        rightTable = rightIterator;

        tableStartIndexMap = startIndexMap;

        firstTableName = firstTable;
        firstTableFilterPredicate = firstFilterPred;
        firstTableNumOfRows = firstNumOfRows;

        currTableName = currTable;
        currTableFilterPredicate = currFilterPred;
        currTableNumOfRows = currNumOfRows;

        naturalJoinPairs = pairs;
        isCartesianJoin = pairs.isEmpty();
    }

    public abstract void resetIterator() throws IOException;

    protected int translateColByTableName(String tableName, int col) {
        return tableStartIndexMap.get(tableName) + database.getRelationByName(tableName).getNewByOldCol(col);
    }

    protected abstract void toJoinTable() throws IOException;

    protected void cartesianJoin() throws IOException {
        int[][] bufferRows = new int[BUFFER_SIZE][];
        int pointer = 0;
        while (true) {
            boolean isRightEnd = true;
            while (rightTable.hasNext()) {
                bufferRows[pointer++] = (int[]) rightTable.next();
                if (pointer < BUFFER_SIZE) continue;
                isRightEnd = false;
                break;
            }
            while (leftTable.hasNext()) {
                int[] leftRow = (int[]) leftTable.next();
                for (int i = 0; i < pointer; i++) addToResultRows(leftRow, bufferRows[i]);
            }
            pointer = 0;
            if (isRightEnd) break;
            resetLeftIterator();
        }
    }

    protected void naturalJoin() throws IOException {
        List<Integer> rightNewCols = new ArrayList();
        NaturalJoinPredicate[] naturalJoinPredicates = new NaturalJoinPredicate[naturalJoinPairs.size()];
        for (int i = 0; i < naturalJoinPairs.size(); i++) {
            ParseElem leftElem = naturalJoinPairs.get(i)[0], rightElem = naturalJoinPairs.get(i)[1];
            int leftCol = translateColByTableName(leftElem.table, leftElem.col);
            int rightCol = database.getRelationByName(rightElem.table).getNewByOldCol(rightElem.col);
            naturalJoinPredicates[i] = new NaturalJoinPredicate(leftCol, rightCol);
            rightNewCols.add(rightCol);
        }

        int[][] bufferRows = new int[BUFFER_SIZE][];
        Map<Integer, Map<Integer, Set<Integer>>> bufferHash = new HashMap(); // k: col, v: map:{ k: colValue, v: list of row_numbers}
        int pointer = 0;

        while (true) {
            boolean isRightEnd = true;
            while (rightTable.hasNext()) {
                bufferRows[pointer] = (int[]) rightTable.next();
                for (int i = 0; i < rightNewCols.size(); i++) {
                    int rightCol = rightNewCols.get(i);
                    int rightColValue = bufferRows[pointer][rightCol];
                    bufferHash.putIfAbsent(rightCol, new HashMap());
                    bufferHash.get(rightCol).putIfAbsent(rightColValue, new HashSet());
                    bufferHash.get(rightCol).get(rightColValue).add(pointer);
                }
                pointer++;
                if (pointer < BUFFER_SIZE) continue;
                isRightEnd = false;
                break;
            }

            while (leftTable.hasNext()) {
                int[] leftRow = (int[]) leftTable.next();

                Set<Integer> rowsInBuffer = new HashSet();
                for (int i = 0; i < naturalJoinPredicates.length; i++) {
                    if (i > 0 && rowsInBuffer.size() == 0) break;
                    NaturalJoinPredicate predicate = naturalJoinPredicates[i];
                    int rightCol = predicate.rightCol, leftColValue = leftRow[predicate.leftCol];
                    if (bufferHash.containsKey(rightCol) && bufferHash.get(rightCol).containsKey(leftColValue)) {
                        if (i == 0) rowsInBuffer = new HashSet(bufferHash.get(rightCol).get(leftColValue));
                        else rowsInBuffer.retainAll(bufferHash.get(rightCol).get(leftColValue));
                    } else if (i > 0) {
                        rowsInBuffer.clear();
                    }
                }
                for (int rowNum : rowsInBuffer) addToResultRows(leftRow, bufferRows[rowNum]);
            }
            bufferHash.clear();
            pointer = 0;
            if (isRightEnd) break;
            resetLeftIterator();
        }
    }

    @Override
    public int getNumOfRows() {
        return numRows;
    }

    protected abstract void addToResultRows(int[] leftRow, int[] rightRow);

    protected void resetLeftIterator() throws IOException {
        if (leftTable instanceof BaseJoin) {
            ((BaseJoin) leftTable).resetIterator();
        } else if (leftTable instanceof Filter) {
            leftTable = null;
            leftTable = new Filter(new Project(new Scan(firstTableName), firstTableNumOfRows, database.getRelationByName(firstTableName).getColsToKeep()), firstTableNumOfRows, firstTableFilterPredicate);
        } else if (leftTable instanceof Project) {
            leftTable = null;
            leftTable = new Project(new Scan(firstTableName), firstTableNumOfRows, database.getRelationByName(firstTableName).getColsToKeep());
        } else if (leftTable instanceof Scan) {
            leftTable = null;
            leftTable = new Scan(firstTableName);
        }
    }

    protected void resetRightIterator() throws IOException {
        if (rightTable instanceof Filter) {
            rightTable = null;
            rightTable = new Filter(new Project(new Scan(currTableName), currTableNumOfRows, database.getRelationByName(currTableName).getColsToKeep()), currTableNumOfRows, currTableFilterPredicate);
        } else if (rightTable instanceof Project) {
            rightTable = null;
            rightTable = new Project(new Scan(currTableName), currTableNumOfRows, database.getRelationByName(currTableName).getColsToKeep());
        } else if (rightTable instanceof Scan) {
            rightTable = null;
            rightTable = new Scan(currTableName);
        }
    }

}
