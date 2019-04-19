package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.interfaces.RowsCounter;

import java.util.Iterator;
import java.util.function.Predicate;

public class Filter implements Iterator<int[]>, RowsCounter {

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

    @Override
    public int getNumOfRows() {
        return numRows;
    }
}