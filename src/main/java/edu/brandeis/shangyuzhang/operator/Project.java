package edu.brandeis.shangyuzhang.operator;

import java.util.Iterator;

public class Project implements Iterator<int[]> {

    private Iterator<int[]> source;
    private int[] colsToKeep;

    private int numRows;

    public Project(Iterator<int[]> input, int numRows, int[] keep) {
        this.source = input;
        this.colsToKeep = keep;
        this.numRows = numRows;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    public int getNumRows() {
        return numRows;
    }

    @Override
    public int[] next() {
        int[] input = source.next();
        int[] row = new int[colsToKeep.length];
        for (int idx = 0; idx < colsToKeep.length; idx++)
            row[idx] = input[colsToKeep[idx]];
        return row;
    }
}