package edu.brandeis.shangyuzhang.operator;

import java.util.Iterator;
import java.util.function.Predicate;

public class Filter implements Iterator<int[]> {

    private Iterator<int[]> source;
    private Predicate pred;
    private int[] row;

    public Filter(Iterator<int[]> input, Predicate p) {
        this.source = input;
        this.pred = p;
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
}