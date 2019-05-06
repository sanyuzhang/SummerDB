package edu.brandeis.shangyuzhang.model;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static edu.brandeis.shangyuzhang.util.Constants.*;

public class FilterPredicate implements Predicate<int[]> {

    public List<String> operators;
    public List<Integer> columns;
    public List<Integer> values;

    private Relation relation;
    private int queryId;

    public FilterPredicate(Relation r, int qid) {
        relation = r;
        queryId = qid;
        operators = new ArrayList();
        columns = new ArrayList();
        values = new ArrayList();
    }

    public void addPredicate(String op, int col, int val) {
        operators.add(op);
        columns.add(col);
        values.add(val);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int size = operators.size();
        for (int i = 0; i < size; i++) {
            sb.append("." + columns.get(i) + operators.get(i) + values.get(i));
        }
        return sb.toString();
    }

    @Override
    public boolean test(int[] projectedRow) {
        int size = operators.size();
        for (int i = 0; i < size; i++) {
            String operator = operators.get(i);
            int newCol = relation.getNewByOldCol(queryId, columns.get(i));
            int value = values.get(i);
            switch (operator) {
                case LESSER:
                    if (projectedRow[newCol] < value) continue;
                    else return false;
                case EQUAL:
                    if (projectedRow[newCol] == value) continue;
                    else return false;
                case GREATER:
                    if (projectedRow[newCol] > value) continue;
                    return false;
            }
        }
        return true;
    }
}
