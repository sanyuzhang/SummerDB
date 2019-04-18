package edu.brandeis.shangyuzhang;

import edu.brandeis.shangyuzhang.util.Database;
import edu.brandeis.shangyuzhang.test.TestDataSet;
import edu.brandeis.shangyuzhang.test.TestDataSetFactory;
import edu.brandeis.shangyuzhang.util.Constants;
import edu.brandeis.shangyuzhang.util.Loader;
import edu.brandeis.shangyuzhang.util.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import static edu.brandeis.shangyuzhang.util.Constants.COMMA;
import static edu.brandeis.shangyuzhang.util.Constants.SUFFIX;

public class Main {

    private static Database database = Database.getInstance();

    private static final boolean isLocal = false;

    private static final String TARGET_DATA_SET = Constants.LARGE;
    private static TestDataSet testDataSet;

    private static void load() {
        database.reset();

        String filePaths = testDataSet.getFilePath();
        database.setLargeDataset(!filePaths.contains("data/x") && !filePaths.contains("data/s") && !filePaths.contains("data/m"));

        String[] tables = testDataSet.getFilePath().split(COMMA);

        for (String table : tables) {
            Loader loader = new Loader(table);
            loader.start();
        }
    }

    private static void load(String filePaths) {
        database.reset();
        database.setLargeDataset(!filePaths.contains("data/x") && !filePaths.contains("data/s") && !filePaths.contains("data/m"));

        String[] tables = filePaths.split(COMMA);

        for (String table : tables) {
            Loader loader = new Loader(table);
            loader.start();
        }
    }

    private static void query() throws IOException {
        long start = System.currentTimeMillis();

        BufferedReader in = new BufferedReader(new FileReader(testDataSet.getQueryPath()));
        int numOfQuery = testDataSet.getNumQuery();
        while (numOfQuery > 0) {
            String[] lines = new String[4];
            for (int i = 0; i < lines.length; i++) {
                lines[i] = in.readLine();
            }
            in.readLine(); // skip the empty line
            Parser parser = new Parser(lines);
            parser.optimize();
            parser.startEngine();
            numOfQuery--;
            database.resetOnNextQuery();
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost - " + (end - start));
    }

    private static void cleanFile() {
        File folder = new File(database.getRootPath());
        File fList[] = folder.listFiles();
        for (File f : fList) {
            if (f.getName().endsWith(SUFFIX)) {
                f.delete();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (isLocal) {
            TestDataSetFactory factory = new TestDataSetFactory();
            testDataSet = factory.createDataSet(TARGET_DATA_SET);
            load();
            query();
        } else {
            Scanner scanner = new Scanner(System.in);
            String inputPaths = scanner.nextLine();

            load(inputPaths);

            int numOfQuery = Integer.parseInt(scanner.nextLine());
            while (numOfQuery > 0) {
                String[] lines = new String[4];
                for (int i = 0; i < lines.length; i++) {
                    lines[i] = scanner.nextLine();
                }

                scanner.nextLine();

                Parser parser = new Parser(lines);
                parser.optimize();
                parser.startEngine();
                numOfQuery--;
                database.resetOnNextQuery();
            }

            scanner.close();
        }
        cleanFile();
    }

}
