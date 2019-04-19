package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.interfaces.RowsCounter;
import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.NaturalJoinPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;
import edu.brandeis.shangyuzhang.util.Catalog;

import java.io.*;
import java.util.*;

import static edu.brandeis.shangyuzhang.util.Constants.SUFFIX;

public class DiskJoin extends BaseJoin implements Iterator<int[]> {

    private DataOutputStream dos;
    private DataInputStream dis;
    private String filename;
    private int rowsRemaining;
    private int numCols = 0;

    private int pointerInBytes;
    private static final int BYTE_SIZE = 1024 * 32;
    private byte[] bytes;

    public DiskJoin(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                    String newTb, List<ParseElem[]> pairs, String firstTable, FilterPredicate firstFilterPred, int firstNumOfRows,
                    String currTable, FilterPredicate currFilterPred, int currNumOfRows) throws IOException {
        super(leftIterator, rightIterator, startColMap, pairs, firstTable, firstFilterPred, firstNumOfRows, currTable, currFilterPred, currNumOfRows);
        filename = database.getRootPath() + newTb + SUFFIX;
        dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        toJoinTable();
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

    public void resetIterator() throws IOException {
        dis = Catalog.openStream(filename, true);

        rowsRemaining = numRows;
        pointerInBytes = 0;
        bytes = new byte[BYTE_SIZE];
        dis.read(bytes);
    }

    protected void toJoinTable() throws IOException {
        if (!isCartesianJoin) naturalJoin();
        else cartesianJoin();
        dos.close();
        rowsRemaining = numRows;
        resetIterator();
    }

    protected void naturalJoin() throws IOException {
        List<Integer> leftNewCols = new ArrayList();
        List<Integer> rightNewCols = new ArrayList();
        NaturalJoinPredicate[] naturalJoinPredicates = new NaturalJoinPredicate[naturalJoinPairs.size()];
        for (int i = 0; i < naturalJoinPairs.size(); i++) {
            ParseElem leftElem = naturalJoinPairs.get(i)[0], rightElem = naturalJoinPairs.get(i)[1];
            int leftCol = translateColByTableName(leftElem.table, leftElem.col);
            int rightCol = database.getRelationByName(rightElem.table).getNewByOldCol(rightElem.col);
            naturalJoinPredicates[i] = new NaturalJoinPredicate(leftCol, rightCol);
            leftNewCols.add(leftCol);
            rightNewCols.add(rightCol);
        }

        int[][] bufferRows = new int[BUFFER_SIZE][];
        Map<Integer, Map<Integer, Set<Integer>>> bufferHash = new HashMap(); // k: col, v: map:{ k: colValue, v: list of row_numbers}
        int pointer = 0;

        int leftSize = ((RowsCounter) leftTable).getNumOfRows();
        int rightSize = ((RowsCounter) rightTable).getNumOfRows();

        if (leftSize >= rightSize) {
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
        } else {
            while (true) {
                boolean isLeftEnd = true;
                while (leftTable.hasNext()) {
                    bufferRows[pointer] = (int[]) leftTable.next();
                    for (int i = 0; i < leftNewCols.size(); i++) {
                        int leftCol = leftNewCols.get(i);
                        int leftColValue = bufferRows[pointer][leftCol];
                        bufferHash.putIfAbsent(leftCol, new HashMap());
                        bufferHash.get(leftCol).putIfAbsent(leftColValue, new HashSet());
                        bufferHash.get(leftCol).get(leftColValue).add(pointer);
                    }
                    pointer++;
                    if (pointer < BUFFER_SIZE) continue;
                    isLeftEnd = false;
                    break;
                }

                while (rightTable.hasNext()) {
                    int[] rightRow = (int[]) rightTable.next();

                    Set<Integer> rowsInBuffer = new HashSet();
                    for (int i = 0; i < naturalJoinPredicates.length; i++) {
                        if (i > 0 && rowsInBuffer.size() == 0) break;
                        NaturalJoinPredicate predicate = naturalJoinPredicates[i];
                        int leftCol = predicate.leftCol, rightColValue = rightRow[predicate.rightCol];
                        if (bufferHash.containsKey(leftCol) && bufferHash.get(leftCol).containsKey(rightColValue)) {
                            if (i == 0) rowsInBuffer = new HashSet(bufferHash.get(leftCol).get(rightColValue));
                            else rowsInBuffer.retainAll(bufferHash.get(leftCol).get(rightColValue));
                        } else if (i > 0) {
                            rowsInBuffer.clear();
                        }
                    }
                    for (int rowNum : rowsInBuffer) addToResultRows(bufferRows[rowNum], rightRow);
                }
                bufferHash.clear();
                pointer = 0;
                if (isLeftEnd) break;
                resetRightIterator();
            }
        }
    }

    protected void addToResultRows(int[] leftRow, int[] rightRow) {
        mergeRow(leftRow, rightRow);
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

}
