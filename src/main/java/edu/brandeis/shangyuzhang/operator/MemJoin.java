package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.model.*;

import java.io.*;
import java.util.*;

public class MemJoin extends BaseJoin implements Iterator<int[]> {

    private int currRow;
    private List<int[]> rows;

    public MemJoin(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                   List<ParseElem[]> pairs, String firstTable, FilterPredicate firstFilterPred, String currTable, FilterPredicate currFilterPred) throws IOException {
        super(leftIterator, rightIterator, startColMap, pairs, firstTable, firstFilterPred, currTable, currFilterPred);
        currRow = 0;
        rows = new ArrayList();
        toJoinTable();
    }

    public boolean isEmptyTable() {
        return numRows == 0;
    }

    @Override
    public boolean hasNext() {
        return currRow < numRows;
    }

    @Override
    public int[] next() {
        return rows.get(currRow++);
    }

    public void resetIterator() {
        currRow = 0;
    }

    protected void toJoinTable() throws IOException {
        if (isCartesianJoin) cartesianJoin();
        else naturalJoin();
        numRows = rows.size();
        resetIterator();
    }

    protected void addToResultRows(int[] leftRow, int[] rightRow) {
        int[] merged = new int[leftRow.length + rightRow.length];
        for (int i = 0; i < merged.length; i++) {
            if (i < leftRow.length) merged[i] = leftRow[i];
            else merged[i] = rightRow[i - leftRow.length];
        }
        rows.add(merged);
    }

}
