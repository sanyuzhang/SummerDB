package edu.brandeis.shangyuzhang.util;

import edu.brandeis.shangyuzhang.model.*;
import edu.brandeis.shangyuzhang.operator.Filter;
import edu.brandeis.shangyuzhang.operator.MemJoin;
import edu.brandeis.shangyuzhang.operator.Project;
import edu.brandeis.shangyuzhang.operator.Scan;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.brandeis.shangyuzhang.util.Constants.*;

public class Parser {

    private Database database = Database.getInstance();

    private static final String EMPTY = "";
    private static final String COMMA = ",";
    private static final String DOT = "\\.";
    private static final String SEMICOLON = ";";
    private static final String SPACES = "\\s";

    private static final String FROM = "FROM";
    private static final String WHERE = "WHERE";
    private static final String AND = "AND";

    private String[] relations;
    private Optimizer optimizer;
    private String joinOrder;
    private List<ParseElem> sumElems;

    public Parser(String[] sql) {
        String from = sql[1];
        parseFrom(from);

        String select = sql[0];
        parseProj(select);

        String where = sql[2];
        parseWhere(where);

        String and = sql[3];
        parsePredicate(and);

        database.initOldNewColsMapping();

//        printRelations();
    }

    private String cleanSqlForParsing(String line, String[] removes) {
        for (String remove : removes) {
            line = line.replaceAll(remove, EMPTY);
        }
        return line;
    }

    private void parseFrom(String line) {
        line = cleanSqlForParsing(line, new String[]{FROM, SPACES});
        relations = line.split(COMMA);
        optimizer = new Optimizer(relations);
    }

    private void parseProj(String line) {
        Pattern p = Pattern.compile("\\((.*?)\\)");
        Matcher m = p.matcher(line);
        sumElems = new ArrayList();
        while (m.find()) {
            sumElems.add(parseTableAndCol(m.group(1)));
        }
    }

    private void parseWhere(String line) {
        line = cleanSqlForParsing(line, new String[]{WHERE, SPACES});
        for (String elem : line.split(AND)) {
            String[] elems = elem.split(EQUAL);
            ParseElem left = parseTableAndCol(elems[0]);
            ParseElem right = parseTableAndCol(elems[1]);
            optimizer.addNaturalJoinPair(left, right);
        }
    }

    private void parsePredicate(String line) {
        line = line.replaceFirst(AND, EMPTY);
        line = cleanSqlForParsing(line, new String[]{SEMICOLON, SPACES});
        line = line.toUpperCase();
        for (String elem : line.split(AND)) {
            if (elem.isEmpty()) continue;
            String operator = null;
            if (elem.contains(LESSER)) {
                operator = LESSER;
            } else if (elem.contains(EQUAL)) {
                operator = EQUAL;
            } else if (elem.contains(GREATER)) {
                operator = GREATER;
            }
            if (operator != null) {
                String[] elems = elem.split(operator);
                ParseElem r = parseTableAndCol(elems[0]);
                int value = Integer.parseInt(elems[1]);
                optimizer.addFilterPredicate(r, operator, value);
            }
        }
    }

    private ParseElem parseTableAndCol(String tableAndCol) {
        String r = tableAndCol.split(DOT)[0];
        int col = Integer.parseInt(tableAndCol.replaceAll("[\\D]", EMPTY));
        database.getRelationByName(r).addColToKeep(col);
        return new ParseElem(r, col);
    }

    private void printRelations() {
        System.out.println("\n=============== PRINT TABLES ===============");
        for (String r : relations) {
            System.out.println("TABLE " + r);
            Relation relation = database.getRelationByName(r);
            for (int c : relation.getColsToKeepSet()) {
                System.out.println("PROJ COL " + c + " - NEW COL" + relation.getNewByOldCol(c));
            }
        }
        System.out.println("=============== PRINT PREDICATES ===============");
        for (Map.Entry<String, FilterPredicate> entry : optimizer.getFilterPredicates().entrySet()) {
            System.out.println("TABLE " + entry.getKey());
            System.out.println(entry.getKey() + entry.getValue().toString());
        }
        System.out.println("=============== PRINT NATURAL JOIN ===============");
        for (ParseElem[] elems : optimizer.getNaturalJoinPairs()) {
            System.out.println(elems[0].table + "." + elems[0].col + " = " + elems[1].table + "." + elems[1].col);
        }
    }

    public void optimize() {
        optimizer.initBestMap();
        joinOrder = optimizer.computeBest(true);
//        System.out.println("================= JOIN ORDER =================");
//        System.out.println(joinOrder);
    }

    public void startEngine() throws IOException {
        Iterator resultIterator = null;
        StringBuilder joinedTableNames = new StringBuilder();

        long[] sums = null;
        int numCols = 0;
        Map<String, Integer> tableToStartColMap = new HashMap();
        String firstTableName = joinOrder.substring(0, 1);
        FilterPredicate firstFilterPred = optimizer.getFilterPredicateByTableName(firstTableName);

        for (int i = 0; i < joinOrder.length(); i++) {
            boolean isLastJoin = i == joinOrder.length() - 1;
            String newTableName = joinOrder.substring(0, i + 1);
            String currTableName = String.valueOf(joinOrder.charAt(i));

            Iterator currIterator = new Scan(currTableName);

            Relation currRelation = database.getRelationByName(currTableName);
            currIterator = new Project(currIterator, currRelation.getColsToKeep());

            FilterPredicate filterPredicate = optimizer.getFilterPredicateByTableName(currTableName);
            if (filterPredicate != null) {
                currIterator = new Filter(currIterator, filterPredicate);
            }

            if (resultIterator != null) {
                List<ParseElem[]> pairs = new ArrayList();
                for (ParseElem[] pair : optimizer.getNaturalJoinPairs()) {
                    if (isNaturalJoinable(joinedTableNames.toString(), currTableName, pair)) pairs.add(pair);
                }
                if (isLastJoin) {
                    tableToStartColMap.put(currTableName, numCols);
                    sums = new MemJoin(resultIterator, currIterator, tableToStartColMap, newTableName, firstTableName, pairs, firstFilterPred, sumElems).getSums();
                    break;
                } else {
                    currIterator = new MemJoin(resultIterator, currIterator, tableToStartColMap, newTableName, firstTableName, pairs, firstFilterPred);
                }
            }
            resultIterator = currIterator;

            joinedTableNames.append(currTableName);
            tableToStartColMap.put(currTableName, numCols);
            numCols += database.getRelationByName(currTableName).getNumOfColsToKeep();
        }

        StringBuilder result = new StringBuilder();
        if (sums != null) {
            int zeroCount = 0;
            for (long s : sums) {
                zeroCount += s == 0 ? 1 : 0;
                result.append(s + ",");
            }
            if (zeroCount == sums.length) {
                result = new StringBuilder();
                for (int i = 0; i < zeroCount; i++) {
                    result.append(",");
                }
            }
        }
        result.deleteCharAt(result.length() - 1);
        System.out.println(result);
    }

    private boolean isNaturalJoinable(String joinedTablesNames, String currTableName, ParseElem[] pair) {
        ParseElem elem1 = pair[0];
        ParseElem elem2 = pair[1];
        if (joinedTablesNames.contains(elem1.table) && currTableName.equals(elem2.table)) return true;
        if (joinedTablesNames.contains(elem2.table) && currTableName.equals(elem1.table)) {
            ParseElem temp = pair[0];
            pair[0] = pair[1];
            pair[1] = temp;
            return true;
        }
        return false;
    }

}
