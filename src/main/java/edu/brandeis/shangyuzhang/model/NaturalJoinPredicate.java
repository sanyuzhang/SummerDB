package edu.brandeis.shangyuzhang.model;

public class NaturalJoinPredicate {

    public int leftCol;
    public int rightCol;

    public NaturalJoinPredicate(int l, int r) {
        leftCol = l;
        rightCol = r;
    }

}
