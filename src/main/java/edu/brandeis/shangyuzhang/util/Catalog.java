package edu.brandeis.shangyuzhang.util;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class Catalog {

    public static DataInputStream openStream(String tableName, boolean isPath) {
        try {
            if (isPath) return new DataInputStream(new FileInputStream(tableName));
            return new DataInputStream(new FileInputStream(Database.getInstance().getRelationPath(tableName)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int numCols(String tableName) {
        return Database.getInstance().getRelationNumCols(tableName);
    }

    public static int numRows(String tableName) {
        return Database.getInstance().getRelationNumRows(tableName);
    }

}
