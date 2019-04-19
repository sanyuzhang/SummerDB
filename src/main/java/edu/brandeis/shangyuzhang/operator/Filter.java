package edu.brandeis.shangyuzhang.operator;

import java.util.Iterator;
import java.util.function.Predicate;

public class Filter implements Iterator<int[]> {

    private Iterator<int[]> source;
    private Predicate pred;
    private int[] row;

    private int numRows;

    public Filter(Iterator<int[]> input, int numRows, Predicate p) {
        this.source = input;
        this.pred = p;
        this.numRows = numRows;
    }

    @Override
    public boolean hasNext() {
        row = null;
        while (source.hasNext()) {
            int[] input = source.next();
            if (pred.test(input)) {
                row = input;
                break;
            }
        }
        return row != null;
    }

    @Override
    public int[] next() {
        return row;
    }

    public int getNumRows() {
        return numRows;
    }
}