package edu.uci.ics.texera.perftest.runme;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.perftest.dictionarymatcher.DictionaryMatcherPerformanceTest;
import edu.uci.ics.texera.perftest.fuzzytokenmatcher.FuzzyTokenMatcherPerformanceTest;
import edu.uci.ics.texera.perftest.keywordmatcher.KeywordMatcherPerformanceTest;
import edu.uci.ics.texera.perftest.nlpextractor.NlpExtractorPerformanceTest;
import edu.uci.ics.texera.perftest.regexmatcher.RegexMatcherPerformanceTest;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;

/**
 * @author Hailey Pan
 */

public class RunTests {
    /*
     * Write Indices Run all performance tests.
     * 
     * Passed in below arguments: 
     * file folder path (where data set stored)
     * result folder path (where performance test results stored) 
     * standard index folder path (where standard index stored) 
     * trigram index folder path(where trigram index stored) 
     * queries folder path (where query files stored)
     * 
     * If above arguments are not passed in, default paths will be used (refer
     * to PerfTestUtils.java) If some of the arguments are not applicable,
     * define them as empty string.
     * 
     * Make necessary changes for arguments, such as query file name, threshold
     * list, and regexQueries
     *
     */
    public static void main(String[] args) {
        
        try {
            PerfTestUtils.setFileFolder(args[0]);
            PerfTestUtils.setResultFolder(args[1]);
            //PerfTestUtils.setStandardIndexFolder(args[2]);
            //PerfTestUtils.setTrigramIndexFolder(args[3]);
            PerfTestUtils.setQueryFolder(args[4]);
        }catch(ArrayIndexOutOfBoundsException e){
            System.out.println("missing arguments will be set to default");
        }

        try {
            PerfTestUtils.deleteDirectory(new File(PerfTestUtils.standardIndexFolder));
            //PerfTestUtils.deleteDirectory(new File(PerfTestUtils.trigramIndexFolder));

            //PerfTestUtils.writeStandardAnalyzerIndices();
            //PerfTestUtils.writeTrigramIndices();

            //List<Double> thresholds = Arrays.asList(0.8, 0.65, 0.5, 0.35);
            //List<String> regexQueries = Arrays.asList("[A-Z][aeiou|AEIOU][A-Za-z]*", "mosquitos?", "v[ir]{2}[us]{2}", "market(ing)?",
                    //"medic(ine|al|ation|are|aid)?");
            // "(\d[4}](\-|\.)\d[1-12](\-|\.)\d[1-31])(.*)(Subject：PRO/AH/EDR.?Zika)\s(virus)(.?.?.?)(Brazil|Dominican\sRepublic)");
            //List<String> regexQueries = Arrays.asList("\\s(Feb|Jun|May|Sep)(\\s|,)[1-9]");
            //List<String> regexQueries = Arrays.asList("medic(ine|al|ation|are|aid)?");
            //List<String> regexQueries = Arrays.asList("(http|ftp)://(\\w+\\.)edu:\\d{1,5}");
            //List<String> regexQueries = Arrays.asList("www\\.(lib|isid|eurosurveillance)(\\.org\\/ViewArticle)*\\.(jpg|aspx)");

             //List<String> regexQueries = Arrays.asList("(\\.\\.)(Feb|Jun|May|Sep|Jul)\\s\\d[0-9]");
           //List<String> regexQueries = Arrays.asList("(http:|ftp:)(/)*twitter\\.com");
            //List<String> regexQueries = Arrays.asList("profile(_backgr|_sideba)(ound_im|r_borde)");
            //List<String> regexQueries = Arrays.asList("(_backgr|_sideba)(ound_im|r_borde)age_url");
           // List<String> regexQueries = Arrays.asList("(blood )(cells |sinus |pressu|serum )re eff");
            //List<String> regexQueries = Arrays.asList("\\b(?:[ABCEGHJKLMNPRSTVXY]\\d[ABCEGHJKLMNPRSTVWXYZ])\\s?(?:\\d[ABCEGHJKLMNPRSTVWXYZ]\\d)\\b");
            //List<String> regexQueries = Arrays.asList("(blood |these )(cells |sinus |pressu|serum )re eff");
            //List<String> regexQueries = Arrays.asList("Jordan (Levitan|Knight'|Jacobso|crying )*face is");
           //List<String> regexQueries = Arrays.asList("Jordan (Le|Knight'|Jacobso)vitan");
            //List<String> regexQueries = Arrays.asList("[0-9]{5}(-[0-9]{4})*[h-z]{5}");
            //List<String> regexQueries = Arrays.asList("re(ab|bc)*tweet(cd|ef)* Bar");
            //List<String> regexQueries = Arrays.asList("tweet");



            //List<String> regexQueries = Arrays.asList("www\\.(lib|isid|eurosurveillance)(.)*\\.(jpg|aspx)");

            //List<String> regexQueries = Arrays.asList("jpeg", "https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.");


            // IP Address
            //List<String> regexQueries = Arrays.asList("(([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3}))(\\.[0-9]{1,3})");

            // Image links
            //List<String> regexQueries = Arrays.asList("http(s?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.)(bmp|gif)");
            //List<String> regexQueries = Arrays.asList("https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.(bmp|png|jpg|gif)");
            //List<String> regexQueries = Arrays.asList("https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.(bmp|gif)");
            //List<String> regexQueries = Arrays.asList("(https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.)(bmp|gif)", "(https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.)(bmp|gif)");

            //List<String> regexQueries = Arrays.asList("(https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.)jpeg", "(https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.)jpeg");
            //List<String> regexQueries = Arrays.asList("https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.jpeg");
            //List<String> regexQueries = Arrays.asList("(https?:\\/\\/[\\w\\.\\/]+\\/[\\w\\.\\/]+\\.)jpeg");
            //Address
            //List<String> regexQueries = Arrays.asList("([0-9]+)((?:st|nd|rd|th)\\s?)(avenue|street|St|Precinct|Ave|Floor)");
            List<String> regexQueries = Arrays.asList("([0-9]+)((?:st|nd|rd|th)\\s?)Floor");
            //List<String> regexQueries = Arrays.asList("([0-9]+)(?:st|nd|rd|th)\\s?Floor");
           // List<String> regexQueries = Arrays.asList("http(s?\\:\\/\\/[a-zA-Z0-9\\-\\.]+\\.[a-zA-Z]{2,7}(?:\\/[\\w\\-]+)*\\.)jpeg");

            //Image files jpg and gif
            //List<String> regexQueries = Arrays.asList("[0-9A-Za-z_ ]+(\\.[jJ][pP][gG]|\\.[gG][iI][fF])");

            //Date
            //List<String> regexQueries = Arrays.asList("(Nov|Jun|Sep|April)(\\s(0[1-9]|[12][0-9]|30))");


            //Titles
            //List<String> regexQueries = Arrays.asList("Dr[.]?|Phd[.]?|MBA\\s");



           // List<String> regexQueries = Arrays.asList("((http|https|ftp)\\:\\/\\/)((((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])|([a-zA-Z0-9_\\-\\.])+\\.(com|net|org|edu|int|mil|gov|arpa|biz|aero|name|coop|info|pro|museum|uk|me)))");

            //List<String> regexQueries = Arrays.asList("((19|20)[0-9]{2}-)(((01|03|05|07|08|10|12)-(0[1-9]|[12][0-9]|3[01]))|(02-(0[1-9]|[12][0-9]))|((04|06|09|11)-(0[1-9]|[12][0-9]|30)))");

           // List<String> regexQueries = Arrays.asList("([1-9]|1[0-2]|0[1-9]){1}(:[0-5][0-9][aApP][mM]){1}");


            //List<String> regexQueries = Arrays.asList("(\\w+([-+\\.']\\w+)*)@gmail\\.com");
            //List<String> regexQueries = Arrays.asList("(\\d{2}\\s{1})((Jan|Feb|Mar|Apr|May|Jun|Jul|Apr|Sep|Oct|Nov|Dec)\\s{1}\\d{4})");

            //List<String> regexQueries = Arrays.asList("[0-9]{10}GBR[0-9]{7}[U,M,F]{1}[0-9]{9}");
            //List<String> regexQueries = Arrays.asList("[a-z0-9!$'*+\\-_]+(\\.[a-z0-9!$'*+\\-_]+)*@([a-z0-9]+(-+[a-z0-9]+)*\\.)+([a-z]{2}|aero|arpa|biz|cat|com|coop|edu|gov|info|int|jobs|mil|mobi|museum|name|net|org|pro|travel)");
            //List<String> regexQueries = Arrays.asList("([a-z0-9!$'*+\\-_]+(\\.[a-z0-9!$'*+\\-_]+)*@([a-z0-9]+(-+[a-z0-9]+)*\\.)+)(cat|com|coop|edu|gov)");

           // List<String> regexQueries = Arrays.asList("([A-Z0-9<]{9}[0-9]{1}[A-Z]{3}[0-9]{7}[A-Z]{1})([0-9]{7}[A-Z0-9<]{14}[0-9]{2})");
            //KeywordMatcherPerformanceTest.runTest("sample_queries.txt");
            //DictionaryMatcherPerformanceTest.runTest("sample_queries.txt");
            //FuzzyTokenMatcherPerformanceTest.runTest("sample_queries.txt", thresholds);
            RegexMatcherPerformanceTest.runTest(regexQueries);
            System.out.println("Regex matching is done!");
           // NlpExtractorPerformanceTest.runTest();

        } catch (StorageException | DataflowException | IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
