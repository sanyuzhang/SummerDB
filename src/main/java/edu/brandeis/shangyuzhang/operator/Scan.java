package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.util.Catalog;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

public class Scan implements Iterator<int[]> {

    private final DataInputStream dis;
    private final int numCols;
    private int rowsRemaining;

    private int pointer;
    private byte[] bytes;

    public Scan(String tb) throws IOException {
        dis = Catalog.openStream(tb, false);
        numCols = Catalog.numCols(tb);
        rowsRemaining = Catalog.numRows(tb);

        pointer = 0;
        bytes = new byte[8 * 1024];
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
            int i = 0;
            int[] row = new int[numCols];
            while (i < numCols) {
                if (pointer < 8 * 1024) {
                    row[i++] = bytes[pointer] << 24 | (bytes[pointer + 1] & 0xFF) << 16 | (bytes[pointer + 2] & 0xFF) << 8 | (bytes[pointer + 3] & 0xFF);
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

}
