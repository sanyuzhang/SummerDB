package edu.brandeis.shangyuzhang.test;

public class MediumDataSet extends TestDataSet {

    public MediumDataSet() {
        rootPath = "data/m/";
        filePath = new StringBuilder();
        for (char c = 'A'; c <= 'P'; c++) {
            filePath.append(rootPath).append(c).append(".csv,");
        }
        filePath.deleteCharAt(filePath.length() - 1);
        numOfQuery = 17;
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
