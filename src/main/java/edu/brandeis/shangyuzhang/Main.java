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
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static edu.brandeis.shangyuzhang.util.Constants.COMMA;
import static edu.brandeis.shangyuzhang.util.Constants.SUFFIX;

public class Main {

    private static Database database = Database.getInstance();

    private static final boolean isLocalTest = false;

    private static TestDataSet testDataSet;
    private static final String TARGET_DATA_SET = Constants.LARGE;
    private static final int NUM_OF_THREADS = 17;

    private static void load(String filePaths) {
        database.setDataPath(filePaths);
        String[] tables = filePaths.split(COMMA);
        for (String table : tables) {
            Loader loader = new Loader(table);
            loader.start();
        }
    }

    private static void query() throws Exception {
        long start = System.currentTimeMillis();
        BufferedReader in = new BufferedReader(new FileReader(testDataSet.getQueryPath()));

        int numOfQuery = testDataSet.getNumQuery();
        database.initMultiThreadsRelations(numOfQuery);

        ExecutorService threadPool = new ThreadPoolExecutor(Math.min(NUM_OF_THREADS, numOfQuery), numOfQuery, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());

        List<Parser> callables = new ArrayList();

        while (numOfQuery > 0) {
            String[] lines = new String[4];
            for (int i = 0; i < lines.length; i++) {
                lines[i] = in.readLine();
            }
            in.readLine();
            Parser parser = new Parser(lines, numOfQuery - 1);
            callables.add(parser);
            numOfQuery--;
        }

        List<Future<String>> futures = threadPool.invokeAll(callables);
        for (Future<String> future : futures) {
            System.out.println(future.get());
        }
        threadPool.shutdown();

        System.out.println("Time Spent - " + (System.currentTimeMillis() - start));
    }

    private static void cleanFile() {
        File folder = new File(database.getRootPath());
        File fList[] = folder.listFiles();
        for (File f : fList) {
            if (f.getName().endsWith(SUFFIX)) f.delete();
        }
    }

    public static void main(String[] args) throws Exception {
        if (isLocalTest) {
            TestDataSetFactory factory = new TestDataSetFactory();
            testDataSet = factory.createDataSet(TARGET_DATA_SET);
            load(testDataSet.getFilePath());
            query();
        } else {
            Scanner scanner = new Scanner(System.in);
            String inputPaths = scanner.nextLine();
            load(inputPaths);

            int numOfQuery = Integer.parseInt(scanner.nextLine());
            database.initMultiThreadsRelations(numOfQuery);

            if (inputPaths.contains("s/")) {
                while (numOfQuery > 0) {
                    String[] lines = new String[4];
                    for (int i = 0; i < lines.length; i++) {
                        lines[i] = scanner.nextLine();
                    }
                    scanner.nextLine();
                    Parser parser = new Parser(lines, numOfQuery - 1);
                    System.out.println(parser.call());
                    numOfQuery--;
                }
                scanner.close();
            } else {
                ExecutorService threadPool = new ThreadPoolExecutor(Math.min(NUM_OF_THREADS, numOfQuery), numOfQuery, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());
                List<Parser> callables = new ArrayList();

                while (numOfQuery > 0) {
                    String[] lines = new String[4];
                    for (int i = 0; i < lines.length; i++) {
                        lines[i] = scanner.nextLine();
                    }
                    scanner.nextLine();
                    Parser parser = new Parser(lines, numOfQuery - 1);
                    callables.add(parser);
                    numOfQuery--;
                }
                scanner.close();

                List<Future<String>> futures = threadPool.invokeAll(callables);
                for (Future<String> future : futures) {
                    System.out.println(future.get());
                }
                threadPool.shutdown();
            }
        }
        cleanFile();
    }

}
