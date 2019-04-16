package edu.brandeis.shangyuzhang.util;

import edu.brandeis.shangyuzhang.model.Relation;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.CharBuffer;

public class Loader {

    private static Database database = Database.getInstance();

    private static final String SUFFIX = ".dat";

    private String oldPath;
    private String newPath;
    private String tableName;

    public Loader(String p) {
        oldPath = p;
        tableName = FilenameUtils.getBaseName(p);
        newPath = FilenameUtils.getPath(p) + tableName + SUFFIX;
    }

    private int getNumOfCols() throws IOException {
        int numCols = 0;
        FileReader fr = new FileReader(oldPath);
        CharBuffer cb = CharBuffer.allocate(4 * 1024);
        while (fr.read(cb) != -1) {
            cb.flip();
            for (int i = 0; i < cb.length(); i++) {
                if (cb.charAt(i) == ',') {
                    numCols++;
                } else if (cb.charAt(i) == '\n') {
                    fr.close();
                    return numCols + 1;
                }
            }
        }
        fr.close();
        return numCols;
    }

    public void start() {
        try {
            int numCols = getNumOfCols();

            FileReader fr = new FileReader(oldPath);
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newPath)));

            CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
            CharBuffer cb2 = CharBuffer.allocate(4 * 1024);

            int colIndex = 0, numRows = 0;
            Relation relation = new Relation(newPath, tableName, numCols, numRows);

            while (fr.read(cb1) != -1) {
                cb1.flip();

                int lastNumberStart = 0;
                for (int i = 0; i < cb1.length(); i++) {
                    if (cb1.charAt(i) == ',') {
                        int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);
                        dos.writeInt(numRead);
                        lastNumberStart = i + 1;
                        relation.addColValToSet(colIndex++ % numCols, numRead);
                    } else if (cb1.charAt(i) == '\n') {
                        int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);
                        dos.writeInt(numRead);
                        lastNumberStart = i + 1;
                        relation.addColValToSet(colIndex++ % numCols, numRead);
                        colIndex %= relation.getNumCols();
                        numRows++;
                    }
                }

                cb2.clear();
                cb2.append(cb1, lastNumberStart, cb1.length());

                CharBuffer tmp = cb2;
                cb2 = cb1;
                cb1 = tmp;

            }

            fr.close();
            dos.close();

            relation.setNumRows(numRows);
            relation.createSizeOfCols();
            database.addRelation(relation);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
