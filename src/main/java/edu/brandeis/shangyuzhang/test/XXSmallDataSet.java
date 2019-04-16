package edu.brandeis.shangyuzhang.test;

public class XXSmallDataSet extends TestDataSet {

    public XXSmallDataSet() {
        rootPath = "data/xxs/";
        filePath = new StringBuilder();
        for (char c = 'A'; c <= 'F'; c++) {
            filePath.append(rootPath).append(c).append(".csv,");
        }
        filePath.deleteCharAt(filePath.length() - 1);
        numOfQuery = 30;
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
