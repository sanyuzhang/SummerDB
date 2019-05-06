package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.interfaces.RowsCounter;
import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.NaturalJoinPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;
import edu.brandeis.shangyuzhang.util.Database;

import java.io.IOException;
import java.util.*;

import static edu.brandeis.shangyuzhang.util.Constants.BUFFER_SIZE;

public abstract class BaseJoin implements RowsCounter {

    protected int numRows;

    private Iterator leftTable;
    private Iterator rightTable;

    private String firstTableName;
    private FilterPredicate firstTableFilterPredicate;
    private int firstTableNumOfRows;

    private String currTableName;
    private FilterPredicate currTableFilterPredicate;
    private int currTableNumOfRows;

    private List<ParseElem[]> naturalJoinPairs;
    private Map<String, Integer> tableStartIndexMap;

    protected boolean isCartesianJoin;

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

    private boolean isRightTableBufferrable() {
        int leftSize = ((RowsCounter) leftTable).getNumOfRows();
        int rightSize = ((RowsCounter) rightTable).getNumOfRows();
        return !database.isDiskJoin() || leftSize >= rightSize;
    }

    protected void cartesianJoin() throws IOException {
        int[][] bufferRows = new int[BUFFER_SIZE][];
        int pointer = 0;
        if (isRightTableBufferrable()) {
            while (true) {
                boolean isRightEnd = true;
                while (rightTable.hasNext()) {
                    bufferRows[pointer++] = (int[]) rightTable.next();
                    if (pointer < BUFFER_SIZE) continue;
                    isRightEnd = false;
                    break;
                }
                if (pointer == 0) break;
                while (leftTable.hasNext()) {
                    int[] leftRow = (int[]) leftTable.next();
                    for (int i = 0; i < pointer; i++) addToResultRows(leftRow, bufferRows[i]);
                }
                pointer = 0;
                if (isRightEnd) break;
                resetLeftIterator();
            }
        } else {
            while (true) {
                boolean isLeftEnd = true;
                while (leftTable.hasNext()) {
                    bufferRows[pointer++] = (int[]) leftTable.next();
                    if (pointer < BUFFER_SIZE) continue;
                    isLeftEnd = false;
                    break;
                }
                if (pointer == 0) break;
                while (rightTable.hasNext()) {
                    int[] rightRow = (int[]) rightTable.next();
                    for (int i = 0; i < pointer; i++) addToResultRows(bufferRows[i], rightRow);
                }
                pointer = 0;
                if (isLeftEnd) break;
                resetRightIterator();
            }
        }
    }

    protected void naturalJoin() throws IOException {
        NaturalJoinPredicate[] naturalJoinPredicates = new NaturalJoinPredicate[naturalJoinPairs.size()];
        for (int i = 0; i < naturalJoinPairs.size(); i++) {
            ParseElem leftElem = naturalJoinPairs.get(i)[0], rightElem = naturalJoinPairs.get(i)[1];
            int leftCol = translateColByTableName(leftElem.table, leftElem.col), rightCol = database.getRelationByName(rightElem.table).getNewByOldCol(rightElem.col);
            naturalJoinPredicates[i] = new NaturalJoinPredicate(leftCol, rightCol);
        }

        List<int[]> bufferRows = new ArrayList(BUFFER_SIZE + 1);
        if (isRightTableBufferrable()) {
            while (true) {
                boolean isRightEnd = true;
                while (rightTable.hasNext()) {
                    bufferRows.add((int[]) rightTable.next());
                    if (bufferRows.size() < BUFFER_SIZE) continue;
                    isRightEnd = false;
                    break;
                }
                if (bufferRows.size() == 0) break;
                Collections.sort(bufferRows, Comparator.comparingInt(o -> o[naturalJoinPredicates[0].rightCol]));
                while (leftTable.hasNext()) {
                    int[] leftRow = (int[]) leftTable.next();
                    List<int[]> candidateRows = new ArrayList();
                    int rightCol = naturalJoinPredicates[0].rightCol, leftColValue = leftRow[naturalJoinPredicates[0].leftCol];
                    int firstRowId = binarySearchOnCol(bufferRows, rightCol, leftColValue);
                    while (firstRowId < bufferRows.size() && bufferRows.get(firstRowId)[rightCol] == leftColValue)
                        candidateRows.add(bufferRows.get(firstRowId++));
                    for (int i = 1; i < naturalJoinPredicates.length; i++) {
                        if (candidateRows.size() == 0) break;
                        NaturalJoinPredicate predicate = naturalJoinPredicates[i];
                        rightCol = predicate.rightCol;
                        leftColValue = leftRow[predicate.leftCol];
                        int finalRightCol = rightCol;
                        Collections.sort(candidateRows, Comparator.comparingInt(o -> o[finalRightCol]));
                        firstRowId = binarySearchOnCol(candidateRows, rightCol, leftColValue);
                        List<int[]> newCandidateRows = new ArrayList();
                        while (firstRowId < candidateRows.size() && candidateRows.get(firstRowId)[rightCol] == leftColValue)
                            newCandidateRows.add(candidateRows.get(firstRowId++));
                        candidateRows = newCandidateRows;
                    }
                    for (int i = 0; i < candidateRows.size(); i++) addToResultRows(leftRow, candidateRows.get(i));
                }
                bufferRows.clear();
                if (isRightEnd) break;
                resetLeftIterator();
            }
        } else {
            while (true) {
                boolean isLeftEnd = true;
                while (leftTable.hasNext()) {
                    bufferRows.add((int[]) leftTable.next());
                    if (bufferRows.size() < BUFFER_SIZE) continue;
                    isLeftEnd = false;
                    break;
                }
                if (bufferRows.size() == 0) break;
                Collections.sort(bufferRows, Comparator.comparingInt(o -> o[naturalJoinPredicates[0].leftCol]));
                while (rightTable.hasNext()) {
                    int[] rightRow = (int[]) rightTable.next();
                    List<int[]> candidateRows = new ArrayList();
                    int leftCol = naturalJoinPredicates[0].leftCol, rightColValue = rightRow[naturalJoinPredicates[0].rightCol];
                    int firstRowId = binarySearchOnCol(bufferRows, leftCol, rightColValue);
                    while (firstRowId < bufferRows.size() && bufferRows.get(firstRowId)[leftCol] == rightColValue)
                        candidateRows.add(bufferRows.get(firstRowId++));
                    for (int i = 1; i < naturalJoinPredicates.length; i++) {
                        if (candidateRows.size() == 0) break;
                        NaturalJoinPredicate predicate = naturalJoinPredicates[i];
                        leftCol = predicate.leftCol;
                        rightColValue = rightRow[predicate.rightCol];
                        int finalLeftCol = leftCol;
                        Collections.sort(candidateRows, Comparator.comparingInt(o -> o[finalLeftCol]));
                        firstRowId = binarySearchOnCol(candidateRows, leftCol, rightColValue);
                        List<int[]> newCandidateRows = new ArrayList();
                        while (firstRowId < candidateRows.size() && candidateRows.get(firstRowId)[leftCol] == rightColValue)
                            newCandidateRows.add(candidateRows.get(firstRowId++));
                        candidateRows = newCandidateRows;
                    }
                    for (int i = 0; i < candidateRows.size(); i++) addToResultRows(candidateRows.get(i), rightRow);
                }
                bufferRows.clear();
                if (isLeftEnd) break;
                resetRightIterator();
            }
        }
    }

    private int binarySearchOnCol(List<int[]> bufferRows, int col, int target) {
        int l = 0, r = bufferRows.size() - 1;
        while (l + 1 < r) {
            int m = (l + r) / 2;
            if (bufferRows.get(m)[col] < target) {
                l = m;
            } else {
                r = m;
            }
        }
        if (bufferRows.get(l)[col] == target) return l;
        if (bufferRows.get(r)[col] == target) return r;
        return bufferRows.size();
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
            leftTable = new Filter(new Scan(firstTableName, database.getRelationByName(firstTableName).getColsToKeep()), firstTableNumOfRows, firstTableFilterPredicate);
        } else if (leftTable instanceof Scan) {
            leftTable = null;
            leftTable = new Scan(firstTableName, database.getRelationByName(firstTableName).getColsToKeep());
        }
    }

    protected void resetRightIterator() throws IOException {
        if (rightTable instanceof Filter) {
            rightTable = null;
            rightTable = new Filter(new Scan(currTableName, database.getRelationByName(currTableName).getColsToKeep()), currTableNumOfRows, currTableFilterPredicate);
        } else if (rightTable instanceof Scan) {
            rightTable = null;
            rightTable = new Scan(currTableName, database.getRelationByName(currTableName).getColsToKeep());
        }
    }

}
