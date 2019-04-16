package edu.brandeis.shangyuzhang.model;

import java.util.*;

import static edu.brandeis.shangyuzhang.util.Constants.*;

public class Relation implements Estimator {

    private String filePath;
    private String tableName;
    private int numCols;
    private int numRows;
    private int[] minCols;
    private int[] maxCols;
    private int[] sizeOfCols;
    private Set<Integer>[] uniqueNumCols;

    private int[] colsToKeep;
    private Set<Integer> colsToKeepSet;
    private Map<Integer, Integer> oldNewColsMapping;

    public Relation(String path, String tn, int c, int r) {
        filePath = path;
        tableName = tn;
        numCols = c;
        numRows = r;

        minCols = new int[numCols];
        Arrays.fill(minCols, Integer.MAX_VALUE);

        maxCols = new int[numCols];
        Arrays.fill(maxCols, Integer.MIN_VALUE);

        sizeOfCols = new int[numCols];
        uniqueNumCols = new HashSet[numCols];
        for (int i = 0; i < numCols; i++) uniqueNumCols[i] = new HashSet();

        colsToKeepSet = new HashSet();
    }

    public void resetOnNextQuery() {
        colsToKeep = null;
        colsToKeepSet.clear();
        oldNewColsMapping.clear();
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public void initOldNewColsMapping() {
        int size = colsToKeepSet.size();
        colsToKeep = new int[size];
        oldNewColsMapping = new HashMap(size);
        for (int c : colsToKeepSet) {
            colsToKeep[--size] = c;
        }
        Arrays.sort(colsToKeep);
        int newIndex = 0;
        for (int c : colsToKeep) {
            oldNewColsMapping.put(c, newIndex++);
        }
    }

    public void addColValToSet(int index, int value) {
        minCols[index] = Math.min(minCols[index], value);
        maxCols[index] = Math.max(maxCols[index], value);
        uniqueNumCols[index].add(value);
    }

    public void createSizeOfCols() {
        for (int i = 0; i < numCols; i++) {
            sizeOfCols[i] = uniqueNumCols[i].size();
        }
        clearUniqueNumsColsSet();
    }

    private void clearUniqueNumsColsSet() {
        uniqueNumCols = null;
//        System.gc();
    }

    public int getNumCols() {
        return numCols;
    }

    public int getNumRows() {
        return numRows;
    }

    public String getTableName() {
        return tableName;
    }

    public void addColToKeep(int col) {
        colsToKeepSet.add(col);
    }

    public int[] getColsToKeep() {
        return colsToKeep;
    }

    public int getNumOfColsToKeep() {
        return colsToKeep.length;
    }

    public Set<Integer> getColsToKeepSet() {
        return colsToKeepSet;
    }

    public int[] getSizeOfCols() {
        return sizeOfCols;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getNewByOldCol(int oldCol) {
        return oldNewColsMapping.get(oldCol);
    }

    @Override
    public double getCardinal(String operator, int oldCol, int value) {
        switch (operator) {
            case LESSER:
                return getCardinalLesserThan(oldCol, value);
            case GREATER:
                return getCardinalGreaterThan(oldCol, value);
            default:
                return getCardinalEqualTo(oldCol);
        }
    }

    @Override
    public double getCardinalEqualTo(int oldCol) {
        return 1.0 * numRows / sizeOfCols[oldCol];
    }

    @Override
    public double getCardinalLesserThan(int oldCol, int value) {
        if (value > minCols[oldCol]) {
            return 1.0 * (value - minCols[oldCol]) / (maxCols[oldCol] - minCols[oldCol]);
        }
        return 0;
    }

    @Override
    public double getCardinalGreaterThan(int oldCol, int value) {
        if (value < maxCols[oldCol]) {
            return 1.0 * (maxCols[oldCol] - value) / (maxCols[oldCol] - minCols[oldCol]);
        }
        return 0;
    }

    @Override
    public double getCardinalNaturalJoin(int oldCol1, Relation r2, int oldCol2) {
        return 1.0 * this.numRows * r2.getNumRows() * Math.min(this.sizeOfCols[oldCol1], r2.getSizeOfCols()[oldCol2]) / this.sizeOfCols[oldCol1] / r2.getSizeOfCols()[oldCol2];
    }

}