package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.interfaces.RowsCounter;
import edu.brandeis.shangyuzhang.util.Catalog;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

public class Scan implements Iterator<int[]>, RowsCounter {

    private final DataInputStream dis;
    private final int numCols;
    private int rowsRemaining;

    private int[] colsToKeep;
    private int numRows;

    private int pointer;
    private byte[] bytes;

    private static final int BUFFER_SIZE = 8 * 1024;

    public Scan(String tb, int[] keep) throws IOException {
        dis = Catalog.openStream(tb, false);
        numCols = Catalog.numCols(tb);
        numRows = Catalog.numRows(tb);
        rowsRemaining = numRows;

        colsToKeep = keep;

        pointer = 0;
        bytes = new byte[BUFFER_SIZE];
        dis.read(bytes);
    }

    public boolean hasNext() {
        if (rowsRemaining == 0) {
            closeStream();
        }
        return rowsRemaining > 0;
    }

    public int[] next() {
        try {
            int col = 0, rowId = 0;

            int rowLen = colsToKeep.length;
            int[] row = new int[rowLen];

            while (col < numCols) {
                if (pointer < BUFFER_SIZE) {
                    if (rowId < rowLen && col == colsToKeep[rowId]) {
                        row[rowId++] = bytes[pointer] << 24 | (bytes[pointer + 1] & 0xFF) << 16 | (bytes[pointer + 2] & 0xFF) << 8 | (bytes[pointer + 3] & 0xFF);
                    }
                    col++;
                    pointer += 4;
                } else {
                    pointer = 0;
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

    public void closeStream() {
        try {
            dis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNumOfRows() {
        return numRows;
    }
}
