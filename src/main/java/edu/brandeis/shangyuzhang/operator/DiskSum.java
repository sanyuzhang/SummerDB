package edu.brandeis.shangyuzhang.operator;

import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DiskSum extends BaseJoin {

    private List<ParseElem> sumElems;
    private long[] sums;
    private int[] sumCols;

    public DiskSum(Iterator<int[]> leftIterator, Iterator<int[]> rightIterator, Map<String, Integer> startColMap,
                   List<ParseElem[]> pairs, String firstTable, FilterPredicate firstFilterPred, String currTable, FilterPredicate currFilterPred, List<ParseElem> sumElms) throws IOException {
        super(leftIterator, rightIterator, startColMap, pairs, firstTable, firstFilterPred, currTable, currFilterPred);
        initSumTools(sumElms);
        toJoinTable();
    }

    private void initSumTools(List<ParseElem> sumElms) {
        sumElems = sumElms;
        sums = new long[sumElems.size()];
        sumCols = new int[sumElems.size()];
        for (int i = 0; i < sumCols.length; i++) {
            ParseElem elem = sumElems.get(i);
            sumCols[i] = translateColByTableName(elem.table, elem.col);
        }
    }

    protected void toJoinTable() throws IOException {
        if (isCartesianJoin) cartesianJoin();
        else naturalJoin();
    }

    protected void addToResultRows(int[] leftRow, int[] rightRow) {
        for (int i = 0; i < sumCols.length; i++) {
            if (sumCols[i] < leftRow.length) sums[i] += leftRow[sumCols[i]];
            else sums[i] += rightRow[sumCols[i] - leftRow.length];
        }
    }

    public long[] getSums() {
        return sums;
    }

    @Override
    public boolean isEmptyTable() {
        return false;
    }

    @Override
    public void resetIterator() {
    }
}