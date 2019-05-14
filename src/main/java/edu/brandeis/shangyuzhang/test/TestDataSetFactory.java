package edu.brandeis.shangyuzhang.test;

import static edu.brandeis.shangyuzhang.util.Constants.*;

public class TestDataSetFactory {

    public TestDataSet createDataSet(String config) {
        TestDataSet dataSet;
        switch (config) {
            case LARGE_TWO:
                dataSet = new LargeTwoDataSet();
                break;
            case LARGE:
                dataSet = new LargeDataSet();
                break;
            case MEDIUM:
                dataSet = new MediumDataSet();
                break;
            case SMALL:
                dataSet = new SmallDataSet();
                break;
            case XSMALL:
                dataSet = new XSmallDataSet();
                break;
            case XXSMALL:
                dataSet = new XXSmallDataSet();
                break;
            case XXXSMALL:
                dataSet = new XXXSmallDataSet();
                break;
            default:
                dataSet = null;
                break;
        }
        return dataSet;
    }

}
