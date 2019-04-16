package edu.brandeis.shangyuzhang.test;

public abstract class TestDataSet {

    protected String rootPath;
    protected StringBuilder filePath;
    protected int numOfQuery;
    protected String queryPath;

    public abstract String getFilePath();

    public abstract int getNumQuery();

    public abstract String getQueryPath();

}
