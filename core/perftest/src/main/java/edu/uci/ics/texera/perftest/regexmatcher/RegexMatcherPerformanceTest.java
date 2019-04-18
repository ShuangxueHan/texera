package edu.uci.ics.texera.perftest.regexmatcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcher;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexSourcePredicate;

import edu.uci.ics.texera.dataflow.source.file.FileSourceOperator;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;
import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.api.constants.test.TestConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/*
 * 
 * @author Zuozhi Wang
 * @author Hailey Pan
 * 
 */
public class RegexMatcherPerformanceTest {

    public static int resultNumber;
    private static String HEADER = "Date, dataset, Average Time, Average Results, Commit Number";
    private static String HEADER1 = "Date, ID, time0, time1, time2,time3, time4, time5, time6, time7, time8, time9, aveTime";
    private static String delimiter = ",";
    private static double totalMatchingTime = 0.0;
    private static double totalWarmupTime = 0.0;
    private static double totalregexMatchTime = 0.0;
    private static int totalRegexResultCount = 0;

    private static String csvFile  = "regex.csv";
    private static String csvFileTest  = "test_regex_sample_tuple_number VS.time.csv";
    public static final String RESULTS = "regex test results";
    /*
     * regexQueries is a list of regex queries.
     * 
     * This function will match the queries against all indices in
     * ./index/trigram/
     * 
     * Test results includes the average runtime of all queries, the average
     * number of results. These results are written to
     * ./perftest-files/results/regex.csv.
     * 
     * CSV file example: 
     * Date,                dataset,      Average Time, Average Results, Commit Number
     * 09-09-2016 00:54:29, abstract_100, 0.2798,       69.80
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /scripts/dashboard/build.py
     * 
     */


    public static void runTest(List<String> regexQueries)
            throws TexeraException, IOException {         
        // Gets the current time for naming the cvs file
        String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());

        // Writes results to the csv file
        PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFile), HEADER);
        BufferedWriter fileWriter = Files.newBufferedWriter
                (PerfTestUtils.getResultPath(csvFile), StandardOpenOption.APPEND);
        for(int i=0; i<1; i++) {
            matchRegex(regexQueries);
            //System.out.println("Regex matching Time: "+totalMatchingTime);
        }

        fileWriter.append("\n");
        fileWriter.append(currentTime + delimiter);
        fileWriter.append(totalRegexResultCount+ delimiter);
        fileWriter.append(String.format("%.4f", totalMatchingTime /10));
        fileWriter.append(delimiter);
        fileWriter.append(String.format("%.4f", totalWarmupTime / 10));
        fileWriter.append(delimiter);
        fileWriter.append(String.format("%.4f", totalregexMatchTime / 10));
        fileWriter.append(delimiter);
        fileWriter.flush();
        fileWriter.close();



        System.out.println("Regex matching is done! Time: "+totalMatchingTime / 5);

   
    }

    /*
     *         This function does match for a list of regex queries
     */
    public static void matchRegex(List<String> regexes) throws TexeraException, IOException {
        // source file
        List<String> attributeNames =Arrays.asList("content");
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        Path tempFolderPath = Paths.get("./core/perftest/src/main/resources/sample-data-files");
        //Path tempFile1Path = tempFolderPath.resolve("promed.txt");
        //Path tempFile1Path = tempFolderPath.resolve("test.txt");
       // Path tempFile1Path = tempFolderPath.resolve("test_short.txt");
         //Path tempFile1Path = tempFolderPath.resolve("abstract_100K.txt");
        //Path tempFile1Path = tempFolderPath.resolve("twitter_200Ke.json");
        Path tempFile1Path = tempFolderPath.resolve("sampleTwitters14M.json");
       // Path tempFile1Path = tempFolderPath.resolve("sample_twitter_10K.json");
        //Path tempFile1Path = tempFolderPath.resolve("sample_twitter_5.json");
        //Path tempFile1Path = tempFolderPath.resolve("sample_twitter_5e.json");

        FileSourcePredicate predicate = new FileSourcePredicate(
                tempFile1Path.toString(), attrName);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        fileSource.open();
        PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFileTest), HEADER1);
        BufferedWriter fileWriter1 = Files.newBufferedWriter
                (PerfTestUtils.getResultPath(csvFileTest));



        //System.out.println("break "+regexes.size());

        long startMatchTime = System.currentTimeMillis();

        // double regexMatchTime = 0.0;

        Tuple tuple;

       // List<Tuple> resultsBA = new ArrayList<>();

        //regexMatcher.open();
        //String tupleCurrentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
        //for(int k=0; k<regexes.size();k++) {
            //System.out.println("k =  "+ k);

        List<Tuple> resultsAB = new ArrayList<>();
        RegexPredicate regexPredicate = new RegexPredicate(regexes.get(0), attributeNames, RESULTS);
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);

        regexMatcher.setInputOperator(fileSource);
        regexMatcher.open();
        int idCounter = 0;
        while (idCounter < 10) {

            long regexMatchTimeS = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {

                    //if(k == 1)
                       // regexMatcher.flag = -1;
                //System.out.println("Tuple id "+ i);
                tuple = regexMatcher.getNextTuple();

                if (tuple != null){
                    resultsAB.add(tuple);
                    //System.out.println("Tuple id "+ i);
                }

            }

            long regexMatchTimeE = System.currentTimeMillis();
            double oneTupleMatchTime = (regexMatchTimeE - regexMatchTimeS) / 1000.0;
            // oneTupleMatchTime = regexMatchTime;
            //averageMatchTime += regexMatchTime;
            //fileWriter1.append(tupleCurrentTime + delimiter);
            fileWriter1.append(idCounter + delimiter);
            fileWriter1.append(oneTupleMatchTime + delimiter);
            fileWriter1.append("\n");
            // fileWriter1.append(delimiter);
            idCounter++;

        }
        regexMatcher.close();
        fileSource.close();
        System.out.println("Regex matching  :" + resultsAB.size());

        //System.out.println("Regex matching BA" + resultsBA.size());

        long endMatchTime =System.currentTimeMillis();
        double matchTime = (endMatchTime - startMatchTime) / 1000.0;
        //System.out.println("Regex matching "+results.size() +" "+ matchTime);
        totalMatchingTime += matchTime;
        //totalRegexResultCount += counter;

        fileWriter1.flush();
        fileWriter1.close();


    }
    public static void matchRegex(List<String> regexes, String tableName) throws TexeraException, IOException {

        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);

        for(String regex: regexes){
            // analyzer should generate grams all in lower case to build a lower
            // case index.
            RegexSourcePredicate predicate = new RegexSourcePredicate(regex, attributeNames, tableName, SchemaConstants.SPAN_LIST);
            RegexMatcherSourceOperator regexSource = new RegexMatcherSourceOperator(predicate);

            long startMatchTime = System.currentTimeMillis();
            regexSource.open();
            int counter = 0;
            Tuple nextTuple = null;
            while ((nextTuple = regexSource.getNextTuple()) != null) {
                ListField<Span> spanListField = nextTuple.getField(SchemaConstants.SPAN_LIST);
                List<Span> spanList = spanListField.getValue();
                counter += spanList.size();
            }
            regexSource.close();
            long endMatchTime = System.currentTimeMillis();
            double matchTime = (endMatchTime - startMatchTime) / 1000.0;
            totalMatchingTime += matchTime;
            totalRegexResultCount += counter;
        }
    }
}