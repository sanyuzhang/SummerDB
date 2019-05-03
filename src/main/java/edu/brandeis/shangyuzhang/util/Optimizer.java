package edu.brandeis.shangyuzhang.util;

import edu.brandeis.shangyuzhang.model.FilterPredicate;
import edu.brandeis.shangyuzhang.model.ParseElem;
import edu.brandeis.shangyuzhang.model.Relation;

import java.util.*;

public class Optimizer {

    private String[] tables;
    private Map<String, String> best;

    private Map<String, Double> cardinalMap;

    private List<ParseElem[]> naturalJoinPairs;
    private Map<String, List<ParseElem[]>> naturalJoinPairMap;
    private Map<String, FilterPredicate> filterPredicates;

    private Database database = Database.getInstance();

    public Optimizer(String[] tbs) {
        tables = tbs;
        best = new HashMap();
        cardinalMap = new HashMap();
        naturalJoinPairs = new ArrayList();
        naturalJoinPairMap = new HashMap();
        filterPredicates = new HashMap();
    }

    public FilterPredicate getFilterPredicateByTableName(String tb) {
        return filterPredicates.get(tb);
    }

    public List<ParseElem[]> getNaturalJoinPairs() {
        return naturalJoinPairs;
    }

    public void addNaturalJoinPair(ParseElem left, ParseElem right) {
        String pairKey;
        ParseElem[] pair;
        if (left.table.compareTo(right.table) < 0) {
            pair = new ParseElem[]{left, right};
            pairKey = left.table + right.table;
        } else {
            pair = new ParseElem[]{right, left};
            pairKey = right.table + left.table;
        }
        naturalJoinPairs.add(pair);
        naturalJoinPairMap.putIfAbsent(pairKey, new ArrayList());
        naturalJoinPairMap.get(pairKey).add(pair);
    }

    public void addFilterPredicate(ParseElem r, String operator, int value) {
        filterPredicates.putIfAbsent(r.table, new FilterPredicate(database.getRelationByName(r.table)));
        filterPredicates.get(r.table).addPredicate(operator, r.col, value);
    }

    public void initBestMap() {
        initBestMap(new StringBuilder(), 0);
    }

    private void initBestMap(StringBuilder key, int start) {
        if (key.length() == 2) {
            best.put(key.toString(), getTwoTablesOrder(key.toString()));
            return;
        }
        for (int i = start; i < tables.length; i++) {
            key.append(tables[i]);
            initBestMap(key, i + 1);
            key.deleteCharAt(key.length() - 1);
        }
    }

    private String getTwoTablesOrder(String rels) {
        Relation r1 = database.getRelationByName(rels.charAt(0));
        Relation r2 = database.getRelationByName(rels.charAt(1));
        if (compareFilteredCardinal(r1, r2) > 0) {
            return r2.getTableName() + r1.getTableName();
        }
        return r1.getTableName() + r2.getTableName();
    }

    private double compareFilteredCardinal(Relation r1, Relation r2) {
        return calculateFilteredCardinal(r1) - calculateFilteredCardinal(r2);
    }

    private double calculateFilteredCardinal(Relation r) {
        if (cardinalMap.containsKey(r.getTableName())) {
            return cardinalMap.get(r.getTableName());
        }
        double cardinality = 0f;
        if (filterPredicates.containsKey(r.getTableName())) {
            FilterPredicate p = filterPredicates.get(r.getTableName());
            int size = p.operators.size();
            for (int i = 0; i < size; i++) {
                String operator = p.operators.get(i);
                int column = p.columns.get(i);
                int value = p.values.get(i);
                if (cardinality == 0) { // first time
                    cardinality = r.getCardinal(operator, column, value);
                } else {
                    cardinality *= r.getCardinal(operator, column, value) / r.getNumRows();
                }
            }
        } else {
            cardinality = r.getNumRows();
        }
        r.setEstimatedCardinality(cardinality);
        cardinalMap.put(r.getTableName(), cardinality);
        return cardinality;
    }

    public String computeBest() {
        StringBuilder rels = new StringBuilder();
        for (String s : tables) rels.append(s);
        return computeBest(rels.toString());
    }

    public String computeBest(String rels) {
        if (best.containsKey(rels)) {
            return best.get(rels);
        }

        String curr = null;
        for (int i = 0; i < rels.length(); i++) {
            char table = rels.charAt(i);
            String rest = rels.replace(table + "", "");
            String internalOrder = computeBest(rest);
            String order1 = table + internalOrder;
            String order2 = internalOrder + table;
            if (compareCost(curr, order1) > 0) curr = order1;
            if (compareCost(curr, order2) > 0) curr = order2;
        }
        best.put(rels, curr);
        return curr;
    }

    private double compareCost(String curr, String order) {
        if (curr == null || curr.isEmpty()) return 1;
        double cardinal1 = calculateNaturalJoinCardinal(curr);
        double cardinal2 = calculateNaturalJoinCardinal(order);
        if (cardinal1 == cardinal2 && database.isReOptOnEqualCost()) {
            for (int i = 0; i < curr.length(); i++) {
                double size1 = database.getRelationByName(curr.substring(i, i + 1)).getEstimatedCardinality();
                double size2 = database.getRelationByName(order.substring(i, i + 1)).getEstimatedCardinality();
                if (size1 == size2) continue;
                return size1 - size2;
            }
        }
        return cardinal1 - cardinal2;
    }

    private double calculateNaturalJoinCardinal(String order) {
        if (cardinalMap.containsKey(order)) {
            return cardinalMap.get(order);
        }
        String joinedTable = String.valueOf(order.charAt(0));
        double cardinality = 0f;
        for (int i = 1; i < order.length(); i++) {
            String currTable = String.valueOf(order.charAt(i));
            double innerCardinality = Double.MAX_VALUE;
            for (char c : joinedTable.toCharArray()) {
                String preTable = String.valueOf(c);
                String pairKey = preTable.compareTo(currTable) < 0 ? preTable + currTable : currTable + preTable;
                innerCardinality = Math.min(innerCardinality, calculateTwoNaturalJoinCardinal(pairKey));
            }
            cardinality += innerCardinality;
            joinedTable += currTable;
        }
        cardinalMap.put(order, cardinality);
        return cardinality;
    }

    private double calculateTwoNaturalJoinCardinal(String pairKey) {
        if (cardinalMap.containsKey(pairKey)) return cardinalMap.get(pairKey);
        Relation r1 = database.getRelationByName(pairKey.charAt(0));
        Relation r2 = database.getRelationByName(pairKey.charAt(1));
        if (naturalJoinPairMap.containsKey(pairKey)) {
            double cardinality = Double.MAX_VALUE;
            for (ParseElem[] pair : naturalJoinPairMap.get(pairKey)) {
                cardinality = Math.min(cardinality, r1.getCardinalNaturalJoin(pair[0].col, r2, pair[1].col));
            }
            cardinalMap.put(pairKey, cardinality);
            return cardinality;
        }
//        double numRows1 = Math.min(r1.getNumRows(), r1.getEstimatedCardinality());
//        double numRows2 = Math.min(r2.getNumRows(), r2.getEstimatedCardinality());
        double numRows1 = r1.getNumRows();
        double numRows2 = r2.getNumRows();
        double cardinality = numRows2 > Double.MAX_VALUE / numRows1 ? Double.MAX_VALUE : numRows1 * numRows2;
        cardinalMap.put(pairKey, cardinality);
        return cardinality;
    }

}