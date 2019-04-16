package edu.brandeis.shangyuzhang.operator;

import java.util.Iterator;

public class Project implements Iterator<int[]> {

    private Iterator<int[]> source;
    private int[] colsToKeep;

    public Project(Iterator<int[]> input, int[] keep) {
        this.source = input;
        this.colsToKeep = keep;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
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