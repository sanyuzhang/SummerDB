package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.model.*;
import edu.brandeis.shangyuzhang.util.Catalog;
import edu.brandeis.shangyuzhang.util.Database;

import java.io.*;
import java.util.*;

import static edu.brandeis.shangyuzhang.util.Constants.SUFFIX;

public class Join implements Iterator<int[]> {

    private DataOutputStream dos;
    private DataInputStream dis;
    private String filename;
    private int rowsRemaining;
    private int numRows;
    private int numCols = 0;

    private int pointerInBytes;
    private static final int BYTE_SIZE = 1024 * 32;
    private byte[] bytes;

    private Iterator diskTable;
    private Iterator memTable;

    private String firstTableName;
    private FilterPredicate firstTableFilterPredicate;

    private List<ParseElem[]> naturalJoinPairs;
    private Map<String, Integer> tableToStartColMap;

    private boolean isCartesianJoin;

    private List<ParseElem> sumElems;
    private long[] sums;
    private int[] sumCols;

    private static final int BUFFER_SIZE = 1024 * 32;
    private Database database = Database.getInstance();

    public Join(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                String newTb, String rightTb, List<ParseElem[]> pairs, FilterPredicate firstFilterPred) throws IOException {
        initializeJoin(leftIterator, rightIterator, startColMap, newTb, rightTb, pairs, firstFilterPred);
        toJoinTable();
    }

    public Join(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                String newTb, String rightTb, List<ParseElem[]> pairs, FilterPredicate firstFilterPred, List<ParseElem> sumElms) throws IOException {
        initializeJoin(leftIterator, rightIterator, startColMap, newTb, rightTb, pairs, firstFilterPred);
        initSumTools(sumElms);
        toJoinTable();
    }

    private void initializeJoin(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                                String newTb, String firstTable, List<ParseElem[]> pairs, FilterPredicate firstFilterPred) throws IOException {
        filename = database.getRootPath() + newTb + SUFFIX;

        memTable = leftIterator;
        diskTable = rightIterator;

        tableToStartColMap = startColMap;

        firstTableName = firstTable;
        firstTableFilterPredicate = firstFilterPred;

        naturalJoinPairs = pairs;
        isCartesianJoin = pairs.isEmpty();

        dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
    }

    public void closeStream() {
        try {
            dis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        if (rowsRemaining == 0) {
            closeStream();
        }
        return rowsRemaining > 0;
    }

    @Override
    public int[] next() {
        try {
            int i = 0;
            int[] row = new int[numCols];
            while (i < numCols) {
                if (pointerInBytes < BYTE_SIZE) {
                    row[i++] = bytes[pointerInBytes] << 24 | (bytes[pointerInBytes + 1] & 0xFF) << 16 | (bytes[pointerInBytes + 2] & 0xFF) << 8 | (bytes[pointerInBytes + 3] & 0xFF);
                    pointerInBytes += 4;
                } else {
                    pointerInBytes = 0;
                    dis.read(bytes);
                }
            }
            rowsRemaining--;
            return row;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void initializeDataStream() throws IOException {
        dis = Catalog.openStream(filename, true);

        rowsRemaining = numRows;
        pointerInBytes = 0;
        bytes = new byte[BYTE_SIZE];
        dis.read(bytes);
    }

    private void initSumTools(List<ParseElem> sumElms) {
        sumElems = sumElms;
        sums = new long[sumElems.size()];
        sumCols = new int[sumElems.size()];
        for (int i = 0; i < sumCols.length; i++) {
            ParseElem elem = sumElems.get(i);
            sumCols[i] = translateColByTableName(elem.table, elem.col);
        }
    }

    private int translateColByTableName(String tableName, int col) {
        return tableToStartColMap.get(tableName) + database.getRelationByName(tableName).getNewByOldCol(col);
    }

    private void toJoinTable() throws IOException {
        if (isCartesianJoin) cartesianJoin();
        else naturalJoin();
        dos.close();
        rowsRemaining = numRows;
        initializeDataStream();
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
            mergeRow(memRow, diskRow);
        }
    }

    private void mergeRow(int[] memRow, int[] diskRow) {
        try {
            if (numCols == 0)
                numCols = memRow.length + diskRow.length;
            for (int i = 0; i < numCols; i++) {
                if (i < memRow.length) dos.writeInt(memRow[i]);
                else dos.writeInt(diskRow[i - memRow.length]);
            }
            numRows++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long[] getSums() {
        return sums;
    }

    private void resetRightIterator() throws IOException {
        if (memTable instanceof Scan) {
            memTable = null;
            memTable = new Scan(firstTableName);
        } else if (memTable instanceof Project) {
            memTable = null;
            memTable = new Project(new Scan(firstTableName), database.getRelationByName(firstTableName).getColsToKeep());
        } else if (memTable instanceof Filter) {
            memTable = null;
            memTable = new Filter(new Project(new Scan(firstTableName), database.getRelationByName(firstTableName).getColsToKeep()), firstTableFilterPredicate);
        } else if (memTable instanceof Join) {
            ((Join) memTable).initializeDataStream();
        }
    }

}
