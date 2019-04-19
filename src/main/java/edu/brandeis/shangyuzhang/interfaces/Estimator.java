package edu.brandeis.shangyuzhang.interfaces;

import edu.brandeis.shangyuzhang.model.Relation;

public interface Estimator {

    double getCardinal(String operator, int oldCol, int value);

    double getCardinalEqualTo(int oldCol);

    double getCardinalLesserThan(int oldCol, int value);

    double getCardinalGreaterThan(int oldCol, int value);

    double getCardinalNaturalJoin(int oldCol1, Relation r2, int oldCol2);

}
