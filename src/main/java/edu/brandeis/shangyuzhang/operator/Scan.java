package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.interfaces.RowsCounter;
import edu.brandeis.shangyuzhang.util.Catalog;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import static edu.brandeis.shangyuzhang.util.Constants.BUFFER_SIZE;

public class Scan implements Iterator<int[]>, RowsCounter {

    private DataInputStream dis;
    private int numCols;
    private int rowsRemaining;

    private int newNumCols;
    private int[] colsToKeep;
    private int numRows;

    private int pointer;
    private byte[] bytes;
    private int jump;

    public Scan(String tb, int[] keep) throws IOException {
        dis = Catalog.openStream(tb, false);
        numCols = Catalog.numCols(tb);
        numRows = Catalog.numRows(tb);
        rowsRemaining = numRows;

        colsToKeep = keep;
        newNumCols = colsToKeep.length;
        jump = 4 * (numCols - colsToKeep[newNumCols - 1] - 1);

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
            int col = 0, newCol = 0;
            int[] row = new int[newNumCols];
            while (col < numCols) {
                if (pointer < BUFFER_SIZE) {
                    if (newCol < newNumCols) {
                        if (col == colsToKeep[newCol]) {
                            row[newCol++] = bytes[pointer] << 24 | (bytes[pointer + 1] & 0xFF) << 16 | (bytes[pointer + 2] & 0xFF) << 8 | (bytes[pointer + 3] & 0xFF);
                        }
                        col++;
                        pointer += 4;
                    } else {
                        pointer += jump;
                        if (pointer < BUFFER_SIZE) break;
                        else {
                            pointer -= BUFFER_SIZE;
                            dis.read(bytes);
                            break;
                        }
                    }
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
