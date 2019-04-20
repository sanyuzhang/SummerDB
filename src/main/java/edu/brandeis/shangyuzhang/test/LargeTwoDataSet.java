package edu.brandeis.shangyuzhang.test;

public class LargeTwoDataSet extends TestDataSet {

    public LargeTwoDataSet() {
        rootPath = "data/l2/";
        filePath = new StringBuilder();
        for (char c = 'A'; c <= 'F'; c++) {
            filePath.append(rootPath).append(c).append(".csv,");
        }
        filePath.deleteCharAt(filePath.length() - 1);
        numOfQuery = 3;
        queryPath = rootPath + "queries.sql";
    }

    @Override
    public String getFilePath() {
        return filePath.toString();
    }

    @Override
    public int getNumQuery() {
        return numOfQuery;
    }

    @Override
    public String getQueryPath() {
        return queryPath;
    }
}
