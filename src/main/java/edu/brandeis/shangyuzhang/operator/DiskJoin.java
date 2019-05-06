package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;
import edu.brandeis.shangyuzhang.util.Catalog;

import java.io.*;
import java.util.*;

import static edu.brandeis.shangyuzhang.util.Constants.BYTE_SIZE;
import static edu.brandeis.shangyuzhang.util.Constants.SUFFIX;

public class DiskJoin extends BaseJoin implements Iterator<int[]> {

    private DataOutputStream dos;
    private DataInputStream dis;
    private String filename;
    private int rowsRemaining;
    private int numCols = 0;

    private int pointerInBytes;
    private byte[] bytes;

    public DiskJoin(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                    String newTb, List<ParseElem[]> pairs, String firstTable, FilterPredicate firstFilterPred, int firstNumOfRows,
                    String currTable, FilterPredicate currFilterPred, int currNumOfRows, int qid) throws IOException {
        super(leftIterator, rightIterator, startColMap, pairs, firstTable, firstFilterPred, firstNumOfRows, currTable, currFilterPred, currNumOfRows, qid);
        filename = database.getRootPath() + newTb + qid + SUFFIX;
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
