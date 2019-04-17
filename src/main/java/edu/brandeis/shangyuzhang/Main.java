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

import static edu.brandeis.shangyuzhang.util.Constants.SUFFIX;

public class Main {

    private static Database database = Database.getInstance();

    private static final String TARGET_DATA_SET = Constants.XXXSMALL;
    private static TestDataSet testDataSet;

    private static void load() {
        long start = System.currentTimeMillis();

        database.reset();
        String[] tables = testDataSet.getFilePath().split(",");

        for (String table : tables) {
//            Thread loader = new Thread(new MultiThreadLoader(table));
            Loader loader = new Loader(table);
            loader.start();
        }

        long end = System.currentTimeMillis();
        System.out.println("Loading took time: " + (end - start) + "\n");
    }

    private static void query() throws IOException {
        long start = System.currentTimeMillis();
//        System.gc();

        BufferedReader in = new BufferedReader(new FileReader(testDataSet.getQueryPath()));
        int numOfQuery = testDataSet.getNumQuery();
        while (numOfQuery > 0) {
            String[] lines = new String[4];
            for (int i = 0; i < lines.length; i++) {
                lines[i] = in.readLine();
                if (lines[i] == null) return;
            }
            in.readLine(); // skip the empty line
            Parser parser = new Parser(lines);
            parser.optimize();
            parser.startEngine();
            numOfQuery--;
            database.resetOnNextQuery();
        }

        long end = System.currentTimeMillis();
        System.out.println("\nQuerying took time: " + (end - start));
    }

    public static void main(String[] args) throws IOException {

        TestDataSetFactory factory = new TestDataSetFactory();
        testDataSet = factory.createDataSet(TARGET_DATA_SET);

        load();
        query();

        cleanFile();

//        Scanner scanner = new Scanner(System.in);
//        String numQuery = scanner.nextLine();
//        String query = scanner.nextLine();

    }

    private static void cleanFile() throws IOException {
        File folder = new File(database.getRootPath());
        File fList[] = folder.listFiles();
        for (File f : fList) {
            if (f.getName().endsWith(SUFFIX)) {
                f.delete();
            }
        }
    }

}
