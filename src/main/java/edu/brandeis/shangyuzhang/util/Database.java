package edu.brandeis.shangyuzhang.util;

import edu.brandeis.shangyuzhang.model.Relation;

import java.util.*;

public class Database {

    private static Database instance;
    private static String rootPath;
    private static Map<String, Relation> relations;

    private boolean isLargeDataset;

    private Database() {
        relations = new HashMap<>();
    }

    public static Database getInstance() {
        if (instance == null) {
            instance = new Database();
        }
        return instance;
    }

    public void addRelation(Relation r) {
        relations.put(r.getTableName(), r);
    }

    public Relation getRelationByName(char tableName) {
        return this.getRelationByName(tableName + "");
    }

    public Relation getRelationByName(String tableName) {
        return relations.get(tableName);
    }

    public void initOldNewColsMapping() {
        for (Relation r : relations.values()) r.initOldNewColsMapping();
    }

    public String getRelationPath(String tableName) {
        return relations.get(tableName).getFilePath();
    }

    public int getRelationNumCols(String tableName) {
        return relations.get(tableName).getNumCols();
    }

    public int getRelationNumRows(String tableName) {
        return relations.get(tableName).getNumRows();
    }

    public boolean isLargeDataset() {
        return isLargeDataset;
    }

    public void setLargeDataset(boolean largeDataset) {
        isLargeDataset = largeDataset;
    }

    public void resetOnNextQuery() {
        for (Relation r : relations.values()) {
            r.resetOnNextQuery();
        }
    }

    public static String getRootPath() {
        return rootPath;
    }

    public static void setRootPath(String rootPath) {
        Database.rootPath = rootPath;
    }

    public void reset() {
        isLargeDataset = false;
        relations.clear();
    }

}
