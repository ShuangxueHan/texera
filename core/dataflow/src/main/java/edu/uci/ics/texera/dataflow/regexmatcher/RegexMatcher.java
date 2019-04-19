package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.regexmatcher.SubRegexGraph.DirectedGraph;
import edu.uci.ics.texera.dataflow.regexmatcher.SubRegexGraph.GraphInterface;
import edu.uci.ics.texera.dataflow.regexmatcher.label.LabeledRegexProcessor;
import edu.uci.ics.texera.dataflow.regexmatcher.label.LabledRegexNoQualifierProcessor;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
/**
 * Created by chenli on 3/25/16.
 * 
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {

    public enum RegexType {
        NO_LABELS, LABELED_WITHOUT_QUALIFIER, LABELED_WITH_QUALIFIERS
    }


    /*
     * Regex pattern for determining if the regex has labels.
     * Match "<" in the beginning, and ">" in the end.
     * Between the brackets "<>", there are one or more number of characters,
     *   but cannot be "<" or ">", or the "\" escape character.
     *
     * For example:
     *   "<drug1>": is a label
     *   "<drug\>1": is not a label because the closing bracket is escaped.
     *   "<a <drug> b>" : only the inner <drug> is treated as a label
     *
     * TODO:
     * this regex can't handle escape inside a bracket pair:
     * <a\>b>: the semantic of this regex is, the label itself can be "a>b"
     */
    public static final String CHECK_REGEX_LABEL = "<[^<>\\\\]*>";

    /*
     * Regex pattern for determining if the regex has qualifiers.
     *
     * TODO:
     * this regex doesn't handle qualifiers correct.
     * It only allows alphabets, digits, and backets.
     * But some characters like "_", "-", "=" doesn't have special meaning
     *   and shouldn't be treated as qualifiers.
     */
    public static final String CHECK_REGEX_QUALIFIER = "[^a-zA-Z0-9<> ]";

    public static final int MAX_TUPLES_FOR_STAT_COLLECTION = 1000;
    private final RegexPredicate predicate;
    private RegexType regexType;
    private Pattern regexPattern;
    //private Pattern subRegexPattern;
    LabeledRegexProcessor labeledRegexProcessor;
    LabledRegexNoQualifierProcessor labledRegexNoQualifierProcessor;

    // no idea what is variable used to
    //public static final String LABEL_REGEX_PLACE_HOLDER = "(########)";
    // List of granular sub-regexes.
    //List<AbstractSubSequence> coreSubSequences = new ArrayList<>();
    List<SubRegex> coreSubRegexes = new ArrayList<>();
    List<List<SubRegex>> subRegexContainer = new ArrayList<>();
    //List<SubRegex> coreSubSequences = new ArrayList<>();
    String fieldValue;
    //List<Pattern> coreSubPatterns = new ArrayList<>();
    // The main container of all sub-regexes.

    GraphInterface<SubRegex> subRegexDirGraph = new DirectedGraph<>();
    //int resultCount = 0;
    int filterCount = 0;
    int warmCount = 0;
    int[][] minLengthMatrix;
    int[][] maxLengthMatrix;
    //long totalWarmUpTimes = 0;
    public int flag = 0;

    SubRegex startSubRegex;
    // public static String fieldValue = null;

    private SubRegex mainRegex;
    // The candidate plans
    //List<QueryPlan> queryPlans = new ArrayList<>();
    private boolean addResultAttribute = false;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataflowException {
        //System.out.println("4.1.setUp");
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }

        Schema inputSchema = inputOperator.getOutputSchema();

        this.addResultAttribute = predicate.getSpanListName() != null;
        Schema.checkAttributeExists(inputSchema, predicate.getAttributeNames());
        if (addResultAttribute) {
            Schema.checkAttributeNotExists(inputSchema, predicate.getSpanListName());
        }

        outputSchema = transformToOutputSchema(inputOperator.getOutputSchema());

        findRegexType();
        // Check if labeled or unlabeled
        if (this.regexType == RegexType.NO_LABELS) {
            regexPattern = predicate.isIgnoreCase() ?
                    Pattern.compile(predicate.getRegex(), Pattern.CASE_INSENSITIVE)
                    : Pattern.compile(predicate.getRegex());

            // set up the needed data structures for optimization and dynamic
            // evaluation
            // 1. break the regex into a number of sub-regexes that are called CoreSubRegexes or CSRs here.
            //breakIntoCoreSubregexes(null);
            //2. initializeOffsetMatrix
            //initializeOffsetMatrix();
            // 2. maintain the subregexes in a graph.
            //generateExpandedSubRegexes();
            //generateSubRegexesCotainStar();
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            labeledRegexProcessor = new LabeledRegexProcessor(predicate);
        } else {
            labledRegexNoQualifierProcessor = new LabledRegexNoQualifierProcessor(predicate);
        }
    }

    /*
     * Determines the type of the regex: no_label / labeled_with_qualifier / labeled_without_qualifier
     */
    private void findRegexType() {
        Matcher labelMatcher = Pattern.compile(CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        if (!labelMatcher.find()) {
            regexType = RegexType.NO_LABELS;
            return;
        }
        Matcher qualifierMatcher = Pattern.compile(CHECK_REGEX_QUALIFIER).matcher(predicate.getRegex());
        if (qualifierMatcher.find()) {
            regexType = RegexType.LABELED_WITH_QUALIFIERS;
        } else {
            regexType = RegexType.LABELED_WITHOUT_QUALIFIER;
        }
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        //System.out.println("6.computeNextMatchingTuple");
        Tuple inputTuple;
        Tuple resultTuple = null;
        if ((inputTuple = inputOperator.getNextTuple()) != null) {
            //System.out.println("6.computeNextMatchingTuple" + cursor);

            if (warmCount < 0) {
                //long stime = System.nanoTime();
                resultTuple = processOneInputTuple(inputTuple);
                //long etime = System.nanoTime();
                //System.out.println("processOneInputTupleTime: " + (etime - stime)/1000);
            } else {
                //long stime = System.nanoTime();
                resultTuple = processOneInputTuplewithStatic(inputTuple);
                //long etime = System.nanoTime();
                //System.out.println("processOneInputTuplewithStaticTime: " + (etime - stime)/1000);

            }
            warmCount++;
        }

        //subRegexDirGraph.getDepthFirstTraversal(startSubRegex);
        //System.out.println("resultCount" + resultCount);
        //System.out.println("filterCount" + filterCount);
        return resultTuple;
    }

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     *
     * @param inputTuple document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     * in the document
     * @throws DataflowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws DataflowException {
        // System.out.println("processOneInputTuple  ");

        if (inputTuple == null) {
            //System.out.println("processOneInputTuple  ");
            return null;
        }
        fieldValue = inputTuple.getField("content").getValue().toString();
        List<Span> matchingResults = new ArrayList<>();
        if (this.regexType == RegexType.NO_LABELS) {
            // only one subregex

            if (coreSubRegexes.isEmpty()) {
                // 1. matching with regex
                //System.out.println("processOneInputTuple  ");

                matchingResults = computeMatchingResultsWithPattern(fieldValue, predicate, regexPattern);

                // 2. matching with NFA
                //
                //matchingResults = computeMatchingResultsWithPatternNFA(inputTuple, predicate);

            } else {




                 //System.out.println("matchingResultsSize= " + matchingResults.size());
               /* System.out.println();
                for(int i=0;i<timeList.size();i++)
                    System.out.print("  time  "+ timeList.get(i));
                System.out.println(" ");
                System.out.println("----------------------------------------------------------------------- ");
                */
                //initializeSRG(subMatchMap);





            }


        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            matchingResults = labeledRegexProcessor.computeMatchingResults(inputTuple);
        } else {
            matchingResults = labledRegexNoQualifierProcessor.computeMatchingResults(inputTuple);
        }
        if (matchingResults.isEmpty()) {
            return null;
        }
        Tuple.Builder tupleBuilder = new Tuple.Builder(inputTuple);
        if (addResultAttribute) {
            tupleBuilder.add(predicate.getSpanListName(), AttributeType.LIST, new ListField<Span>(matchingResults));
        }
        //matchingResults = null;
        return tupleBuilder.build();
    }

    public Tuple processOneInputTuplewithStatic(Tuple inputTuple) throws DataflowException {
       // System.out.println("processOneInputTuplewithStatic ");
        //List<Span> currentResults = new ArrayList<>();
        if (inputTuple == null) {
            return null;
        }
        fieldValue = inputTuple.getField("content").getValue().toString();
        List<Span> matchingResults = new ArrayList<>();


        List<SubRegex> coreSubregexList = new ArrayList<>();
        for(SubRegex sub: coreSubRegexes){
            coreSubregexList.add(sub);
        }


        if (this.regexType == RegexType.NO_LABELS) {
            if (coreSubRegexes.isEmpty()) {
                // 1. matching with JAVA regex
                matchingResults = computeMatchingResultsWithPattern(fieldValue, predicate, regexPattern);
                // 2. matching with NFA
                // matchingResults = computeMatchingResultsWithPatternNFA(inputTuple, predicate);
            } else {
                //updateQueryPlan();


                //printAllSubRegexes();

                /* matching regex for each tuple
                *
                * */

                //FOR AB->BA

                //matchingResults = computeMatchingResultsWithPattenAB(fieldValue, coreSubRegexes, predicate, -1);
                //  BA
/*
                flag = -1;
                coreSubregexList.get(0).selectivity = 0.2;
                coreSubregexList.get(1).selectivity = 0.0;
                coreSubregexList.sort(Comparator.comparing(SubRegex::getSelectivity));

                printAllSubRegexes();
                matchingResults = computeMatchingResultsWithPattenAB(fieldValue, coreSubregexList, predicate, flag);
                flag = 0;


 */


                /*BA->AB*/


                // FOR BA
/*
                 flag = -1;
                    coreSubregexList.get(0).selectivity = 0.2;
                    coreSubregexList.get(1).selectivity = 0.1;

                    coreSubregexList.sort(Comparator.comparing(SubRegex::getSelectivity));

                    printAllSubRegexes();
                    matchingResults = computeMatchingResultsWithPatterCoreSubregexes(fieldValue, coreSubregexList, predicate, flag);

                //matchingResults = computeMatchingResultsWithPatterCoreSubregexes(fieldValue, coreSubRegexes, predicate, flag);
                // for AB
                flag = 1;
                coreSubregexList.get(0).selectivity = 0.2;
                coreSubregexList.get(1).selectivity = 0.0;
                coreSubregexList.sort(Comparator.comparing(SubRegex::getSelectivity));
                //printAllSubRegexes();
                matchingResults = computeMatchingResultsWithPatterCoreSubregexes(fieldValue, coreSubRegexes, predicate, flag);
*/

                //matchingResults = computeMatchingResultsWithPattern(fieldValue, predicate, regexPattern,)

                /* matching with subRegexDirGraph*/
                //subRegexDirGraph.resetVertices();
                //matchingResults = computeMatchingResultsWithPattenSubRegexDirGraph(inputTuple, subRegexDirGraph, predicate);

                // matching with ACB* and AC+ABBC

                if (mainRegex.complexity == SubRegex.ComplexityLevel.High)
                    //matchingResults = computeMatchingResultsCoreSubregxesSTARB(inputTuple, predicate);
                 //matchingResults = computeMatchingResultsCoreSubregxesSTARA();
               // matchingResults = computeMatchingResultsWithPatternC();
                //matchingResults = computeMatchingResultsWithPatternD();
                    //matchingResults = computeMatchingResultsWithPattenABC(1);
                    matchingResults = compyteMatchingResultsWithABListC();

                //else
                    // matching  without STAR
                    //matchingResults = computeMatchingResultsWithCoreSubregxes(fieldValue, predicate);



            }

            //System.out.println("matchingResultSize: " + matchingResults.size());
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            matchingResults = labeledRegexProcessor.computeMatchingResults(inputTuple);
        } else {
            matchingResults = labledRegexNoQualifierProcessor.computeMatchingResults(inputTuple);
        }
        if (matchingResults.isEmpty()) {
            return null;
        }
        Tuple.Builder tupleBuilder = new Tuple.Builder(inputTuple);
        if (addResultAttribute) {
            tupleBuilder.add(predicate.getSpanListName(), AttributeType.LIST, new ListField<Span>(matchingResults));
        }

        return tupleBuilder.build();
    }

    /**
     * Java Regex
     * @param fieldValue
     * @param predicate
     * @param pattern
     * @return
     */
    public static List<Span> computeMatchingResultsWithPattern(String fieldValue, RegexPredicate predicate, Pattern pattern) {
        ///System.out.println("1 computeMatchingResultsWithPattern"+pattern.toString());
        //pattern = Pattern.compile(StringEscapeUtils.unescapeJava(pattern.toString()));
        //System.out.println("1 computeMatchingResultsWithPattern"+pattern.toString());
        List<Span> matchingResults = new ArrayList<>();

           // System.out.println("1 computeMatchingResultsWithPattern"+fieldValue);
            // Regex matching by JAVA regex matcher

            //int count = 0;
            int start;
            int end;
            //long startTime = System.nanoTime();

            Matcher javaMatcher = pattern.matcher(fieldValue);
            while (javaMatcher.find()) {
                start = javaMatcher.start();
                end = javaMatcher.end();
               //System.out.println("result：" + start + "--" + end + " " + fieldValue.substring(start, end));
                matchingResults.add(
                        new Span("content", start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            }
            //long endTime = System.nanoTime();
            //long time = (endTime - startTime)/1000;
            //System.out.print("  Time  " + time);



        return matchingResults;
    }



    public List<Span> computeMatchingResultsWithPatternC(){
        List<Span> matchingResults;

        SubRegex startSubRegex = coreSubRegexes.get(0);
        SubRegex endSubRegex = coreSubRegexes.get(1);
        List<SubRegex> subStars = new ArrayList<>();
        subStars.add(coreSubRegexes.get(2));

        matchingResults = computeResultsWithPatternAC(fieldValue, startSubRegex, endSubRegex, subStars);
        return  matchingResults;
    }

    /**
     * Matching regex with  AC OR CA to verify the occurrences of B*.
     * @param fieldValue the string
     * @param startSubRegex A
     * @param endSubRegex C
     * @param subStars B*
     * @return
     */
    private List<Span> computeResultsWithPatternAC(String fieldValue, SubRegex startSubRegex, SubRegex endSubRegex, List<SubRegex> subStars) {
        //System.out.println("2 computeMatchingResultsWithPattern :"+ currentResults.size());
        List<Span> matchingResults = new ArrayList<>();
        boolean flag = true;
        if(startSubRegex.originalSubId > endSubRegex.originalSubId) {
            flag = false;
            //SubRegex curr = startSubRegex;
            //startSubRegex = endSubRegex;
            //endSubRegex = curr;

        }
        StringBuffer sb = new StringBuffer(fieldValue);
        String reverseFieldValue = sb.reverse().toString();


        String starstring = combineStars(subStars, true);
        //System.out.println("starstring : "+ starstring);
        Pattern pattern = Pattern.compile(starstring);
        Matcher starMatcher = pattern.matcher(fieldValue);

        //List<Span> startSubResults = new ArrayList<>();

        Pattern startPattern = startSubRegex.regexPatern;
        Matcher startJavaMatcher = startPattern.matcher(fieldValue);

        Pattern endPattern = endSubRegex.regexPatern;
        Matcher endJavaMatcher = endPattern.matcher(fieldValue);

        //C OR A
        //int updateStart = 0;
        while (startJavaMatcher.find()) {
            int start = startJavaMatcher.start();
            int end = startJavaMatcher.end();
            //System.out.println("resultcC：" + start + " " + fieldValue.substring(start, end));
            if(!flag){
                //A
                endPattern = endSubRegex.reverseSubRegex.regexPatern;
                //System.out.println("endPattern : "+ endPattern.toString());
                endJavaMatcher = endPattern.matcher(reverseFieldValue);

                if(endJavaMatcher.find(fieldValue.length()-start)) {
                    int nextStart = endJavaMatcher.start();
                    int nextEnd = endJavaMatcher.end();
                    //matchingResults.add(
                            //new Span("content", fieldValue.length()-nextStart, end, predicate.getRegex(), fieldValue.substring(fieldValue.length()-nextStart, end)));

                    //System.out.println("resultA：" +  nextEnd + " "  + fieldValue.substring(fieldValue.length()-nextStart, start));
                    starMatcher.reset(fieldValue.substring(fieldValue.length()-nextStart, start));

                    if(starMatcher.matches()){
                        matchingResults.add(
                                new Span("content", fieldValue.length()-nextEnd, end, predicate.getRegex(), fieldValue.substring(fieldValue.length()-nextEnd, end)));

                    }
                    /*
                    // reverse B*
                    starMatcher.reset(reverseField);
                    if (starMatcher.find(fieldValue.length()-end)) {
                        //System.out.println("resultC：" +(starMatcher.start()-1) + " " + starMatcher.end());
                        //System.out.println("resultC：" +(fieldValue.length()-start) + " " + nextStart);
                        int finalStart = reverseFieldValue.length() - nextEnd;
                        if (starMatcher.end() == nextStart && (fieldValue.length() - start) == starMatcher.start()) {
                            //System.out.println("result：" + fieldValue.substring(finalStart, end));
                            matchingResults.add(
                                    new Span("content", finalStart, end, predicate.getRegex(), fieldValue.substring(finalStart, end)));
                        } else if (starMatcher.end() == nextStart && (fieldValue.length() - start) == (starMatcher.start() - 1)) {
                            matchingResults.add(
                                    new Span("content", finalStart, end, predicate.getRegex(), fieldValue.substring(finalStart, end)));
                        }

                    }


                     */


                }

            }else {

                    int updateEnd = end;
                    while(endJavaMatcher.find(updateEnd)){
                        int nextStart = endJavaMatcher.start();
                        int nextEnd = endJavaMatcher.end();
                        //System.out.println("resultA：" +  nextEnd + " "  + fieldValue.substring(nextStart, nextEnd));
                        //B*
                        if (starMatcher.find(end)) {
                        //System.out.println("resultB：" + fieldValue.substring(starMatcher.start(), starMatcher.end()));
                        //System.out.println("start B：" + starMatcher.start() + " == " + end + " end " + starMatcher.end() + " == " + nextStart);
                        if (starMatcher.start() == end) {
                            if (starMatcher.end() == nextStart) {
                                //System.out.println("result：" + fieldValue.substring(start, nextEnd));
                                matchingResults.add(
                                        new Span("content", start, nextEnd, predicate.getRegex(), fieldValue.substring(start, nextEnd)));
                                //Next C
                            }
                            updateEnd = nextEnd;


                        } else break;
                    } else break;
                }



            }


        }

        return matchingResults;
    }


    /**For A(B*C) or C with reverse (AB*)
     *
     * @param fieldValue
     * @param coreSubRegexes
     * @param predicate
     * @param flag
     * @return
     */
    public  List<Span> computeMatchingResultsWithPattenAB(String fieldValue, List<SubRegex> coreSubRegexes, RegexPredicate predicate, int flag) {
        //System.out.println("3 computeMatchingResultsWithPatterCoreSubregexes :"+ fieldValue);
        List<Span> matchingResults = new ArrayList<>();

        SubRegex sub = coreSubRegexes.get(0);
        //Pattern subPattern = sub.regexPatern;
       // Matcher javaMatcher = subPattern.matcher(fieldValue);
        List<Span> matchingResultsA = computeMatchingResultsWithPattern(fieldValue, sub.predicate, sub.regexPatern);
        //System.out.println("matchResult " + sub);
        SubRegex nextSub = coreSubRegexes.get(1);
        Pattern nextPattern = nextSub.regexPatern;
        //Matcher nextJavaMatcher = null;
        Matcher nextJavaMatcher = nextPattern.matcher(fieldValue);

        if(flag == -1){
            //nextPattern = nextSub.getReverseSubRegex().regexPatern;

        }else{
            nextJavaMatcher = nextPattern.matcher(fieldValue);
        }
        /** find BC with matchingList of A */
        for(Span span : matchingResultsA){
            int start = span.getStart();
            int end = span.getEnd();
            /** find BC FROM EACH POSITION  of A */
        //while (javaMatcher.find()) {
            //int start = javaMatcher.start();
            //int end = javaMatcher.end();

            if (flag != -1) {
                //find BC from the end position of A

                if (nextJavaMatcher.find(end) && nextJavaMatcher.start() == end) {
                    end = nextJavaMatcher.end();
                    //System.out.println("Matching: " + start + "--" + end + " " +  fieldValue.substring(start, end));
                    matchingResults.add(
                            new Span("content", start, end, predicate.getRegex(), fieldValue.substring(start, end)));
                }
            } else {
                //System.out.println("Floor: " + start + "--" + end + " " +  fieldValue.substring(start, end));
                /**reverse string to match reverse sub-regex AB */
                /*
                StringBuffer sb = new StringBuffer(fieldValue.substring(0, start));
                String subFieldValue = sb.reverse().toString();
                //System.out.println(start+"--"+end+" "+fieldValue.substring(start, end));
                // System.out.println("subFieldValue: " +" " + nextPattern.toString());
                Matcher subJavaMatcher = nextPattern.matcher(subFieldValue);
                if (subJavaMatcher.find()) {
                    int subStart = subJavaMatcher.start();
                    int subEnd = subJavaMatcher.end();
                    if (subStart == 0 && start >= subEnd) {
                        start = start - subEnd - 1;
                        //System.out.println("Matching: " + start + "--" + end + " " +  fieldValue.substring(start, end));
                        matchingResults.add(
                                new Span("content", start, end, predicate.getRegex(), fieldValue.substring(start, end)));
                    }
                }


                 */
                /**match sub-regex AB from left to right*/
                //Matcher subJavaMatcher = nextPattern.matcher(fieldValue);
                while(nextJavaMatcher.find()){

                    if(nextJavaMatcher.end() == start){
                        //System.out.println("Matching: " + nextJavaMatcher.start() + "--" + end + " " +  fieldValue.substring(nextJavaMatcher.start(), end));
                        matchingResults.add(
                                new Span("content", nextJavaMatcher.start(), end, predicate.getRegex(), fieldValue.substring(nextJavaMatcher.start(), end)));
                    }
                }



            }
        }

        //System.out.println("matchResult " + matchingResults.size());
        return matchingResults;
    }

    /**
     * FOR ABC and CBA
     * @param flag
     * @return
     */

    public  List<Span> computeMatchingResultsWithPattenABC(int flag) {
        //System.out.println("3 computeMatchingResultsWithPatterCoreSubregexes :"+ fieldValue);
        List<Span> matchingResults = new ArrayList<>();
        SubRegex sub = coreSubRegexes.get(0);
        Pattern patternA = sub.regexPatern;
        Matcher javaMatcherA = patternA.matcher(fieldValue);
        //List<Span> matchingResultsA = computeMatchingResultsWithPattern(fieldValue, sub.predicate, sub.regexPatern);

        //System.out.println("matchResult " + sub);
        SubRegex subB = coreSubRegexes.get(1);
        Pattern patternB = subB.regexPatern;
        Matcher javaMatcherB = null;

        SubRegex subC = coreSubRegexes.get(2);
        Pattern patternC = subC.regexPatern;
        Matcher javaMatcherC = null;

        if(flag == -1){
            patternB = subB.getReverseSubRegex().regexPatern;
            patternC = subC.getReverseSubRegex().regexPatern;

        }else{
            javaMatcherB = patternB.matcher(fieldValue);
            javaMatcherC = patternC.matcher(fieldValue);
        }


        /** find next B with matchingList of A or C
        for(int i=0; i<matchingResultsA.size();i++){
            Span span = matchingResultsA.get(i);
            int start = span.getStart();
            int end = span.getEnd();
         */
        // find next B from each matching position A or C
        while (javaMatcherA.find()){
            int start = javaMatcherA.start();
            int end = javaMatcherA.end();

            if(flag != -1) {
                //find B from the end position of A
                if (javaMatcherB.find(end) && javaMatcherB.start() == end) {
                    int endB = javaMatcherB.end();
                    // find C from B
                    if (javaMatcherC.find(endB) && javaMatcherC.start() == endB) {
                        int endC = javaMatcherC.end();
                        //System.out.println("Matching: " + start + "--" + endC + " " + fieldValue.substring(start, endC));
                        matchingResults.add(
                                new Span("content", start, endC, predicate.getRegex(), fieldValue.substring(start, endC)));

                    }
                }
            }
            else {
                //System.out.println("Floor: " + start + "--" + end + " " +  fieldValue.substring(start, end));
                //reverse string to match reverse sub-regex B
                StringBuffer sb = new StringBuffer(fieldValue.substring(0, start));
                String reverseFieldValue = sb.reverse().toString();

                //System.out.println(start+"--"+end+" "+fieldValue.substring(start, end));
                // System.out.println("subFieldValue: " +" " + nextPattern.toString());
                javaMatcherB = patternB.matcher(reverseFieldValue);
                javaMatcherC = patternC.matcher(reverseFieldValue);
                // find B
                if(javaMatcherB.find() && javaMatcherB.start() == 0){

                    int startB = javaMatcherB.start();
                    int endB = javaMatcherB.end();
                    //System.out.println(startB+"--"+endB+" "+fieldValue.substring(start-endB, start));
                    // find A
                    if(javaMatcherC.find(endB) && javaMatcherC.start() == endB) {
                        //System.out.println(javaMatcherC.start()+"--"+endB);

                        int startC = start-javaMatcherC.end();
                        //System.out.println("Matching: " + javaMatcherC.start() + "--" + javaMatcherC.end() + " " +  reverseFieldValue.substring(javaMatcherC.start(), javaMatcherC.end()));
                        matchingResults.add(
                                new Span("content", startC, end, predicate.getRegex(), fieldValue.substring(startC, end)));
                    }

                }
            }
        }

        //System.out.println("matchResult " + matchingResults.size());
        return matchingResults;
    }

    /**
     * FOR Al + Bl + Cl or Cl + Bl + Al

     * @return
     */
    public  List<Span> compyteMatchingResultsWithABCList(){
        List<Span> matchingResults = new ArrayList<>();
        // 5. matching every subregex and merge the matchingResultsList

        //TreeMap<Pair<Double, Double>, Pair<SubRegex, Integer>> subMatchMap = new TreeMap<>();

        //int tupleSize = fieldValue.length();
        //List<Long> timeList = new ArrayList<>();
        //System.out.println("Tuplesize: " + tupleSize);
        //long startTime = 0;
        //long endTime = 0;
        //Pattern subRegexPattern;
        SubRegex sub = coreSubRegexes.get(0);
        matchingResults = computeMatchingResultsWithPattern(fieldValue, sub.predicate, sub.regexPatern);


        //System.out.println("coreSubRegexes : " + coreSubRegexes.size());
        for (int i = 1; i < coreSubRegexes.size(); i++) {

            //subRegexPattern = coreSubPatterns.get(i);
            if(matchingResults.isEmpty()) break;

            SubRegex nextSub = coreSubRegexes.get(i);

            //System.out.println("coreSubRegexes : " + sub+ "size : " + matchingResults.size());
            List<Span> currentResults = computeMatchingResultsWithPattern(fieldValue, nextSub.predicate, nextSub.regexPatern);
            if(currentResults.isEmpty()) {
                matchingResults.clear();
                break;
            }

            /* FOR Statistics
            endTime = System.currentTimeMillis();

            Double time = (endTime - startTime) * 1.0;

            if (currentResults.size() == 0) {
                sub.stats.addStatsSubRegexFailure(time, currentResults.size());
            } else
                sub.stats.addStatsSubRegexSuccess(time, currentResults.size());

            sub.selectivity = sub.stats.getSelectivity();
            sub.selectivity = currentResults.size() * 1.0 / tupleSize;




            //sub.stats.addStatsSubRegexSuccess(matchTime, currentResults.size());
            Pair<Double, Double> matchMap1 = new MutablePair<>(sub.selectivity, time);
            Pair<SubRegex, Integer> matchMap2 = new MutablePair<>(sub, currentResults.size());
            subMatchMap.put(matchMap1, matchMap2);

            //System.out.println("Warmup: "+ currentResults.size() + sub + " selectivity:  "+ sub.selectivity  + " time: " + time);

             */
            //if (i == 0) matchingResults = currentResults;
            //System.out.println("  occ  " + currentResults.size());
            //System.out.println(nextSub.originalSubId + "subregex : " + sub.originalSubId);
            if(sub.originalSubId > nextSub.originalSubId){
                matchingResults = computeSpanIntersection(currentResults, matchingResults);
            }else
                matchingResults = computeSpanIntersection(matchingResults, currentResults);

            sub = nextSub;
            //System.out.println("matchingResultsSize= " + matchingResults.size());

        }
        return  matchingResults;
    }

    /**
     * FOR Al + Bl + Cv
     * @return
     */
    public  List<Span> compyteMatchingResultsWithABListC(){
        boolean flag = true;
        SubRegex sub = coreSubRegexes.get(0);
        List<Span> matchingResults = computeMatchingResultsWithPattern(fieldValue, sub.predicate, sub.regexPatern);


        //System.out.println("coreSubRegexes : " + sub);

        if(matchingResults.isEmpty())
            return matchingResults;

        SubRegex nextSub = coreSubRegexes.get(1);

        //System.out.println("coreSubRegexes : " + sub+ "size : " + matchingResults.size());
        List<Span> currentResults = computeMatchingResultsWithPattern(fieldValue, nextSub.predicate, nextSub.regexPatern);
        if(currentResults.isEmpty()) {
            matchingResults.clear();
            return  matchingResults;
        }

        //System.out.println("  occ  " + currentResults.size());
        //System.out.println(nextSub.originalSubId + "subregex : " + sub.originalSubId);
        Pattern pattern;
        Matcher javaMatcher;
        if(sub.originalSubId > nextSub.originalSubId){
            //flag = false;
            matchingResults = computeSpanIntersection(currentResults, matchingResults);

        }else {
            matchingResults = computeSpanIntersection(matchingResults, currentResults);

        }
        if(matchingResults.isEmpty())
            return matchingResults;

        if(flag){
            pattern = coreSubRegexes.get(2).regexPatern;
            javaMatcher =  pattern.matcher(fieldValue);
        }
        else{
            pattern = coreSubRegexes.get(2).reverseSubRegex.regexPatern;
            StringBuffer sb = new StringBuffer(fieldValue);
            String revFieldValue = sb.reverse().toString();
            javaMatcher =  pattern.matcher(revFieldValue);
        }


         for(int i=0; i<matchingResults.size();i++){
             Span span = matchingResults.get(i);
             int start = span.getStart();
             int end = span.getEnd();
             if(flag){
                 if(javaMatcher.find(end) && javaMatcher.start() == end){

                 }
             }

         }


        //System.out.println("matchingResultsSize= " + matchingResults.size());

        return  matchingResults;
    }
    /**
     *
     * @param inputTuple
     * @param predicate
     * @return
     */
    public List<Span> computeMatchingResultsWithPattenSubRegexDirGraph(Tuple inputTuple, GraphInterface<SubRegex> subRegexDirGraph, RegexPredicate predicate) {
        //System.out.println("3 computeMatchingResultsWithPatterCoreSubregexes :" + predicate.getRegex());
        List<Span> matchingResults = new ArrayList<>();
        List<SubRegex> nextSubList = new ArrayList<>();
        nextSubList.add(startSubRegex);

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataflowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }
            // getBreadthFirstTraversal subRegexDirGraph.getBreadthFirstTraversal(startSubRegex);
            //subRegexDirGraph.resetVertices();
            //System.out.println("startSubRegex :" + startSubRegex);
            List<SubRegex> resultNextSubs = subRegexDirGraph.getMinNextVertexDFS(nextSubList);

            SubRegex nextSub = resultNextSubs.get(resultNextSubs.size()-1);
            System.out.println("start==========NextCoreSubregexes :" + nextSub);

            Pattern subPattern = Pattern.compile(nextSub.predicate.getRegex());
            Matcher javaMatcher = subPattern.matcher(fieldValue);
            while(nextSub != null){
                if(!javaMatcher.find()) {
                    subRegexDirGraph.updateEdgeWeight(startSubRegex, nextSub, 1.0);
                    filterCount++;
                    System.out.println("filtercount:" + filterCount + " subregex  "+ nextSub);
                    break;
                }
                else {
                    javaMatcher.reset();
                    int start = 0;
                    int end = 0;
                    //List<Integer> subPositionList = new ArrayList<>();
                    while (javaMatcher.find()) {
                        start = javaMatcher.start();
                        end = javaMatcher.end();
                        //System.out.println("Subregexes start:" + nextSub + " " + start + "  " + end);
                        //subPositionList.add(start);
                        nextSubList.add(nextSub);
                    }
                    resultNextSubs = subRegexDirGraph.getMinNextVertexDFS(nextSubList);
                    SubRegex nextSub1 = startSubRegex;
                    if (resultNextSubs.isEmpty()) ;
                    else {
                        //String subFieldValue = null;

                        nextSub1 = resultNextSubs.get(resultNextSubs.size() - 1);
                        //System.out.println("1NextCoreSubregexes :" + nextSub1);
                            /*
                            if (nextSub1.startingSubSeqIndex < nextSub.startingSubSeqIndex)
                                subFieldValue = fieldValue.substring(0, start);
                            else
                                subFieldValue = fieldValue.substring(start, fieldValue.length() - 1);
                                */

                        Pattern subPattern1 = Pattern.compile(nextSub1.predicate.getRegex());
                        javaMatcher = subPattern1.matcher(fieldValue);


                    }
                    if(nextSub == nextSub1 || nextSub1 == startSubRegex)
                        break;
                    else nextSub = nextSub1;
                    //}

                }
            }
            //System.out.println("end=========================================");
/*
            Pattern subPattern1 = Pattern.compile(predicate.getRegex());
            Matcher javaMatcher1 = subPattern1.matcher(fieldValue);
            while(javaMatcher1.find()) {
                //filterCount++;
                int start = javaMatcher1.start();
                int end = javaMatcher1.end();
               // System.out.println(" SubRegexDirGraph: " + start + "--" + end + " " + fieldValue.substring(start, end));
                matchingResults.add(
                        new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            }
*/

        }


        return matchingResults;
    }

    public  static List<Span> computeMatchingResultsWithPatternNFA(Tuple inputTuple, RegexPredicate predicate){
      //System.out.println("2 computeMatchingResultsWithPatternNFA :"+ predicate.getRegex());
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {

            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataflowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            RegExp regex = new RegExp(predicate.getRegex());
            Automaton auto = regex.toAutomaton();
            RunAutomaton runAuto = new RunAutomaton(auto);
            for(int i=0; i<fieldValue.length(); i++) {
                int offset = runAuto.run(fieldValue, i);
                if(offset != -1){
                    int start = i;
                    int end = i + offset;
                    //System.out.println("NFA MatchingResults: " + start + "--" + end + " " + fieldValue.substring(start, end));
                    matchingResults.add(
                            new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
                }
            }

        }

        return matchingResults;
    }
    //Regex Without STAR
    // Matching the sub-regex to the start position
    public List<Span> computeMatchingResultsWithCoreSubregxes(String fieldValue, RegexPredicate predicate) {
        System.out.println(" computeMatchingResultsWithCoreSubregxes :" );

        List<Span> matchingResults = new ArrayList<>();
        SubRegex firstSub = coreSubRegexes.get(0);

        Pattern firstPattern = firstSub.regexPatern;
        //Pattern.compile(firstSub.getsubRegexPredicate().getRegex());

        //matchingResults = computeMatchingResultsWithPattern(inputTuple, predicate ,firstPattern);
        Matcher javaMatcher = firstPattern.matcher(fieldValue);
            // long stime = System.nanoTime();
            while(javaMatcher.find()) {
                boolean matchFlag = true;
                int start = javaMatcher.start();
                int end = javaMatcher.end();
                System.out.println("start : " + start + " =="+ fieldValue.substring(start, end) );

                for (int i = 1; i < coreSubRegexes.size(); i++) {

                    SubRegex nextSub = coreSubRegexes.get(i);
                    //System.out.println(firstSub + " " + firstSub.numberOfSubSequences + " "+ firstSub.);
                    if(computeSpanIntersectionWithSubregex("content", fieldValue, firstSub, nextSub, start, end)){
                        //System.out.println(firstSub + " " + nextSub);
                    }
                    else {
                        matchFlag = false;
                        break;
                    }

                }
                if(matchFlag)
                    matchingResults.add(new Span("content", start, end, predicate.getRegex(), fieldValue.substring(start, end)));
                //System.out.println("  matchingResultSize : " + matchingResults.size());

            }

            //long etime = System.nanoTime();
            //System.out.println("  matchingResultTime : " + (etime - stime)/1000);
            //System.out.println("  matchingResultSize : " + matchingResults.size());
        return matchingResults;
    }
    /**
     * bu Yutong
     * find answer based on subregexes with equal length and without HIGH
     * @param attributeName
     * @param fieldValue
     * @param matchSub
     * @param currSub
     * @param matchSpanStart
     * @param matchSpanEnd
     * @return
     */

    private Boolean computeSpanIntersectionWithSubregex(String attributeName, String fieldValue, SubRegex matchSub, SubRegex currSub, int matchSpanStart, int matchSpanEnd){
        //System.out.println(matchSub + "  computeSpanIntersection: "+ currSub);

        //List<Span> finalResults = new ArrayList<>();
        //if(currSub.isLastSubSequence())
        //int offset = matchSub.minLength;
        int matchSubOff = minLengthMatrix[0][matchSub.startingSubSeqIndex];
        int currSubOff = minLengthMatrix[0][currSub.startingSubSeqIndex];

        Pattern pattern = currSub.regexPatern;
        int start = matchSpanStart-matchSubOff;

        if(currSub.startingSubSeqIndex > matchSub.startingSubSeqIndex) {

            int matchEnd = matchSpanEnd;
            //System.out.println(matchSub + " 0 start: " +matchSubOff+ "----- " + matchEnd);
            Matcher currSubMatcher = pattern.matcher(fieldValue);
            if (currSubMatcher.find(matchEnd)) {

                int currStart = currSubMatcher.start()-currSubOff;
                int end = currSubMatcher.end();
                System.out.println("0occ: " + currStart + " " + end);
                System.out.println(currStart + " == " + start);
                if (currStart == start ) {
                    System.out.println("match" + currStart + " == " + start);
                    return true;
                    // finalResults.add(
                    // new Span(attributeName, currSubMatcher.start(), end, predicate.getRegex(), fieldValue.substring(currSubMatcher.start(), end)));
                }

            }
        }
        else{//currSub <- matchSub
            Pattern patternRev = currSub.reverseSubRegex.regexPatern;
            //System.out.println(currSub + " 1start: " + patternRev);

            //Pattern patternRev = Pattern.compile(currSub.getsubRegexPredicate().getRegex());
            int matchStart = matchSpanStart;
            //currSubOff = maxLengthMatrix[0][currSub.startingSubSeqIndex];
            //System.out.println(matchSub + " 1 start: " +matchStart+ "----- " + matchSpan.getEnd());
            int currStart = start + currSubOff;
            //System.out.println(matchSub + " 1start: " + currStart);
            StringBuffer sb = new StringBuffer(fieldValue.substring(0, matchStart));
            String revFieldValue = sb.reverse().toString();
            if(currStart<0) currStart = 0;
            int nextstart = matchStart-currSub.minLength-1;
            if(nextstart <0) nextstart =0;
            //System.out.println("===="+ fieldValue.substring(nextstart, matchStart));

            Matcher currSubMatcher = patternRev.matcher(revFieldValue);
            if(currSubMatcher.find()){
                //System.out.println("1occ: " + currSubMatcher.start() + " " );
                //int end = currSubMatcher.end();
                if(currSubMatcher.start() == 0){
                    //System.out.println(currSubMatcher.start() + " == " + end);
                    return true;
                    //System.out.println("occsubString: " + fieldValue.substring(currStart+currSubMatcher.start(),  currStart+end) );
                    //finalResults.add(
                    //new Span(attributeName, currStart+currSubMatcher.start(), currStart+end, predicate.getRegex(), fieldValue.substring(currStart-1+currSubMatcher.start(),  currStart-1+end)));
                }
            }


        }

        return false;

    }

    /**Regex With STAR A(B*C*D+)E
    * Start from the matching positions list of A, and verify (BCDE)v

     */
    public List<Span> computeMatchingResultsCoreSubregxesSTARA() {
        //System.out.println(" computeMatchingResultsCoreSubregxesSTAR :" + predicate.getRegex());

        SubRegex sub = coreSubRegexes.get(0);

        List<Span> startMatchingResults  = computeMatchingResultsWithPattern(fieldValue, sub.predicate, sub.regexPatern);
        if(startMatchingResults.isEmpty())
            return startMatchingResults;
        //System.out.println( " coreSubRegexes " + startMatchingResults.size());
        for (int startCSRIndex = 0; startCSRIndex < coreSubRegexes.size(); ) {


            //System.out.println( " sub---- " + coreSubRegexes.get(startCSRIndex));
            int endCSRIndex = startCSRIndex+1;
            // STAR|+
            List<SubRegex> subSTARs = new ArrayList<>();
            boolean orderFlag = true;
            for(; endCSRIndex < coreSubRegexes.size(); endCSRIndex++){


                SubRegex endCoreSubRegex = coreSubRegexes.get(endCSRIndex);

                if(! endCoreSubRegex.isSubRegex()){
                    break;
                }

                //System.out.println("endCoreSub " + endCoreSubRegex);
                if(endCoreSubRegex.complexity != SubRegex.ComplexityLevel.High){
                    if((endCoreSubRegex.originalSubId - coreSubRegexes.get(startCSRIndex).originalSubId) < 0)
                        orderFlag = false;

                   // if(minLengthMatrix[startCSRIndex][endCSRIndex] == -1 && !flag)
                    //System.out.println(subSTARs.size() + " ---- " + flag);

                    startMatchingResults = computeSpanIntersectionSTARA(fieldValue, startMatchingResults, endCoreSubRegex, subSTARs, orderFlag);

                    subSTARs.clear();
                    break;
                }
                else {
                    subSTARs.add(endCoreSubRegex);
                }

            }
            startCSRIndex = endCSRIndex+1;

        }

        return startMatchingResults;
    }
    private List<Span> computeSpanIntersectionSTARA(String fieldValue, List<Span> matchingResults, SubRegex endCoreSubRegex, List<SubRegex> subSTARs, boolean flag){
        List<Span> finalResults = new ArrayList<>();
        //System.out.println("2 computeSpanIntersection: "+ matchingResults.size() + " "+ endCoreSubRegex);
        Pattern subSTARPattern;
        String starString = "";
        if(!subSTARs.isEmpty()){

            starString = combineStars(subSTARs, flag);
            if(flag) {
                starString += endCoreSubRegex.regexPatern.toString();

            } else {

                starString += endCoreSubRegex.reverseSubRegex.regexPatern.toString();
            }
            subSTARPattern = Pattern.compile(starString);

        } else{
            if(flag)
            subSTARPattern = endCoreSubRegex.regexPatern;
            else
            subSTARPattern = endCoreSubRegex.reverseSubRegex.regexPatern;
        }

        //System.out.println("subSTARPattern: "+subSTARPattern.toString());


            if(flag) {
                for (int i  = 0; i < matchingResults.size(); i++) {

                    Span span = matchingResults.get(i);
                    int start = span.getEnd();
                    //System.out.println("i= " + i);
                    //System.out.println("start: " + start);

                    // binary search the next start position

                    Matcher javaMatcher = subSTARPattern.matcher(fieldValue);

                    if (javaMatcher.find(start)) {
                        int nextStart = javaMatcher.start();
                        int nextEnd = javaMatcher.end();

                        if (nextStart == start) {
                            //System.out.println( " nextMatch: "+ start + "--" + nextEnd);
                            String value = fieldValue.substring(start, nextEnd);
                            finalResults.add(new Span(span.getAttributeName(), start, nextEnd, starString, value));
                            //System.out.println(span.getStart() + "--" + nextEnd + " true" + value );
                            i++;
                            //break;
                        }/*  else {
                            int minMaxIndex = minMaxSearchMatchResult(matchingResults, nextStart);
                            //System.out.println(i+ "minMaxIndex " + minMaxIndex + "== " + matchingResults.get(minMaxIndex).getStart() );
                            if (i == minMaxIndex)
                                i++;
                            else {
                                i = minMaxIndex;
                                span = matchingResults.get(minMaxIndex);
                                if(start == span.getEnd()){
                                    String value = fieldValue.substring(span.getStart(), nextEnd);
                                    finalResults.add(new Span(span.getAttributeName(), span.getStart(), nextEnd, starString, value));
                                    System.out.println(span.getStart() + "--" + nextEnd + " true" + value );

                                }
                                i++;
                            }

                            //System.out.println( i+ " start: "+ matchingResults.get(i).getStart());
                        }*/

                    } //else i++;
                }
            }else {
                for (int i  = matchingResults.size()-1; i >=0; i--) {
                    //System.out.println(i);
                    Span span = matchingResults.get(i);
                    int start = span.getStart();
                    StringBuffer sb = new StringBuffer(fieldValue.substring(0, start));
                    String subFieldValue = sb.reverse().toString();
                    //System.out.println(start + " " + span.getValue());
                    //System.out.println("subFieldValue: " +" " + subSTARPattern.toString());
                    Matcher subJavaMatcher = subSTARPattern.matcher(subFieldValue);
                    if (subJavaMatcher.find()) {
                        int subStart = subJavaMatcher.start();
                        int subEnd = subJavaMatcher.end();
                        if (subStart == 0 && start >= subEnd) {

                            start = start - subEnd;
                            //System.out.println("Matching: " + start + "--" + span.getEnd() + " " +  fieldValue.substring(start, span.getEnd()));
                            finalResults.add(
                                    new Span("content", start, span.getEnd(), predicate.getRegex(), fieldValue.substring(start, span.getEnd())));

                        }
                    }
                }

        }

        return finalResults;


    }
    private String combineStars(List<SubRegex> subSTARs, boolean flag) {
        String starString = "";
        //Pattern subSTARPattern ;
        for (SubRegex sub : subSTARs) {
            if (flag) {
                starString += sub.regexPatern.toString();
            } else {
                starString += sub.reverseSubRegex.regexPatern.toString();
            }
        }
            //System.out.println("starString: " + starString);
            // subSTARPattern = Pattern.compile(starString);

        return starString;


    }
    /**
     * Match results in two step  AC and AB+C
     * @param inputTuple
     * @param predicate
     * @return
     */
    public List<Span> computeMatchingResultsCoreSubregxesSTARB(Tuple inputTuple, RegexPredicate predicate) {
        //System.out.println(" computeMatchingResultsCoreSubregxesSTARB :" + predicate.getRegex());


        List<Span> startMatchingResults = new ArrayList<>();
        List<Integer> endList = new ArrayList<>();

        //System.out.println( "  " + startMatchingResults.size());
        List<SubRegex> subSTARs = new ArrayList<>();

        SubRegex startSub = coreSubRegexes.get(0);
        for (int i = 1; i < coreSubRegexes.size(); i++) {

            SubRegex sub = coreSubRegexes.get(i);
           // System.out.println( " sub---- " + sub);
           // boolean flag = true;
            if(sub.complexity == SubRegex.ComplexityLevel.High)
                subSTARs.add(sub);
            else {

                SubRegex endCoreSubRegex = coreSubRegexes.get(i);
                if (!endCoreSubRegex.isSubRegex()) {
                    break;
                }
                //System.out.println("endCoreSub " + endCoreSubRegex);

                 //if((endCoreSubRegex.originalSubId - coreSubRegexes.get(startCSRIndex).originalSubId) < 0)
                   //flag = false;
                // if(minLengthMatrix[startCSRIndex][endCSRIndex] == -1 && !flag)
                //System.out.println(subSTARs.size() + " ---- " + flag);
                //AC
                if (!subSTARs.isEmpty()) {
                    List<Span> endMatchingResults = computeMatchingResultsWithCoPatternB(fieldValue, startSub, endCoreSubRegex, endList);

                    //AB*C
                    if (!endList.isEmpty()) {
                        startMatchingResults = computeSpanIntersectionSTARB(fieldValue, endList, endCoreSubRegex, subSTARs, true);
                        startMatchingResults.addAll(endMatchingResults);
                    } else return endMatchingResults;
                    subSTARs.clear();
                    break;
                } else {
                    startSub = combineSubs(startSub, endCoreSubRegex);
                    // startMatchingResults = computeMatchingResultsWithPattern(inputTuple, coSubPredicate, Pattern.compile(coSubString));
                }

            }


        }
        //System.out.println("startMatchingResults " + startMatchingResults.size());
        return startMatchingResults;
    }

    /**
     * Find result for AC in AB*C, for B is not complex e.g., (low)*
     * @param fieldValue
     * @param startSub Subregex A
     * @param endSub Subrege C
     * @param startSubList The start position of A which can NOT match AC
     * @return matchingResult of AC and the other position of A
     */
    private static List<Span> computeMatchingResultsWithCoPatternB(String fieldValue, SubRegex startSub, SubRegex endSub, List<Integer> startSubList) {
        //System.out.println("1 computeMatchingResultsWithPattern"+pattern.toString());
        List<Span> matchingResults = new ArrayList<>();
        //RegexPredicate predicate = startSub.predicate;
        int start;
        int end;
        //long startTime = System.nanoTime();
        Pattern startPattern = startSub.regexPatern;
        Matcher javaMatcher = startPattern.matcher(fieldValue);
        Pattern endPattern = endSub.regexPatern;

        while (javaMatcher.find()) {
            start = javaMatcher.start();
            end = javaMatcher.end();
            //String subFieldValue = fieldValue.substring(end, end + endSub.minLength);
            Matcher endJavaMatcher = endPattern.matcher(fieldValue);
            //System.out.println("MatchingResults: " + start + "--" + end + " "+subFieldValue);
            if (endJavaMatcher.find(end) && endJavaMatcher.start() == end){
                // System.out.println("1 MatchingResults: " + endJavaMatcher.start());
                matchingResults.add(
                        new Span("content", start, end, startSub.predicate.getRegex(), fieldValue.substring(start, end)));


            }
            else
                startSubList.add(end);
            // System.out.println("end " + end);

        }

        return matchingResults;
    }
    private List<Span> computeSpanIntersectionSTARB(String fieldValue, List<Integer> endList, SubRegex endCoreSubRegex, List<SubRegex> subSTARs, boolean flag){
        //System.out.println("computeSpanIntersectionSTARB: "+ endList.size() + " ");
        List<Span> finalResults = new ArrayList<>();
        List<String> subSequences = new ArrayList<>();
        String starString ;
        Pattern subEndPattern = endCoreSubRegex.regexPatern;

        if(!subSTARs.isEmpty()){
            starString = subSTARs.get(0).regexPatern.toString();
            //System.out.println("starString: " + starString);

            PublicRegexp subSTAR = PublicParser.parse(starString, PublicRE2.PERL);
            subSTAR = PublicSimplify.simplify(subSTAR);
            //System.out.println("getOp = " + subSTAR.getOp());
            subSequences = subSTAR.getSequence(subSTAR);
        }

        for (int j = 0; j < endList.size(); j++) {
            int start = endList.get(j);
            int updateStart = start;
            //System.out.println("start= " + start);
            int count = 0;
            while(!subSequences.isEmpty() && count < subSequences.size()) {

                for (; count < subSequences.size(); count++) {
                    starString = subSequences.get(count);
                    //System.out.println("starString = " + starString);
                    String subFieldValue = fieldValue.substring(updateStart, updateStart + starString.length());
                    //subFieldValue = StringEscapeUtils.unescapeJava(subFieldValue);
                    // int escapeCount = starString.length() - subFieldValue.length();

                    if (fieldValue.regionMatches(updateStart, starString, 0, starString.length())) {

                        updateStart += starString.length();
                        //System.out.println("aa : " + updateStart);
                        count = 0;
                        break;
                    }else continue;
                }
            }
            String subEndValue = fieldValue.substring(updateStart, updateStart + endCoreSubRegex.minLength);
            //System.out.println("end start : " + updateStart + " " + subEndValue);
            Matcher endJavaMatcher = subEndPattern.matcher(subEndValue);
            if (endJavaMatcher.matches()) {
                finalResults.add(new Span("content", start, start + endJavaMatcher.end(), subEndValue, subEndValue));
                System.out.println("bb : " + (start + endJavaMatcher.end()));
                break;
            }


        }
        //System.out.println("finalResults: " + finalResults.size());
        return finalResults;


    }

    /**
     * position List A and position List C to verfy B or find positions of B
     * CAB/ ACB
     * @return
     */
    public List<Span> computeMatchingResultsWithPatternD(){

        SubRegex sub = coreSubRegexes.get(0);
        List<Span> matchingResults = computeMatchingResultsWithPattern(fieldValue, sub.predicate, sub.regexPatern);
         new ArrayList<>();

        SubRegex nextSub = coreSubRegexes.get(1);
        Pattern nextSubRegexPattern = nextSub.regexPatern;
        List<Span> currentResults = computeMatchingResultsWithPattern(fieldValue, nextSub.predicate, nextSubRegexPattern);

        SubRegex starSub = coreSubRegexes.get(2);
        if(Math.abs(sub.originalSubId-nextSub.originalSubId) == 1){
            // AB
            matchingResults = computeSpanIntersection(currentResults, matchingResults);
            //BA
        }
        // ACB*
        else if(sub.originalSubId > nextSub.originalSubId+1 ){
                matchingResults = computeSpanIntersection(currentResults, matchingResults, starSub);
                //System.out.println("matchingResultsSize= " + matchingResults.size());
        }
        //CAB*
        else  matchingResults = computeSpanIntersection(matchingResults, currentResults, starSub);


        return matchingResults;
    }
    public List<Span> computeMatchingResultsWithPatternE(){
        List<Span>  matchingResults = new ArrayList<>();

        return matchingResults;
    }

    @Override
    protected void cleanUp() throws DataflowException {        
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        Schema.Builder outputSchemaBuilder = new Schema.Builder(inputSchema[0]);
        if (addResultAttribute) {
            outputSchemaBuilder.add(predicate.getSpanListName(), AttributeType.LIST);
        }
        return outputSchemaBuilder.build();
    }

    private void breakIntoCoreSubregexes(List<String> labels){
       // System.out.println(" breakIntoCoreSubregexes ");
        //int labelsIndex = 0;

        PublicRegexp re = PublicParser.parse(predicate.getRegex(), PublicRE2.PERL);
        re = PublicSimplify.simplify(re);
        //System.out.println("1 breakIntoCoreSubregexes ");
        //List<String> subRegexSequences = new ArrayList<>();

        int numberOfCoreSubRegexes = 0;
        int minMainRegexLength = PublicRegexp.computeMinLength(re);
        int maxMainRegexLength = PublicRegexp.computeMaxLength(re);
        SubRegex.ComplexityLevel mainRegexComplexity = SubRegex.ComplexityLevel.Low;
        if(re.getOp() != PublicRegexp.PublicOp.CONCAT){
            //String mainReverseRegex = PublicRegexp.reverseDeepCopy(re);
            SubRegex coreSubRegex = new SubRegex(predicate, 0, 1,  mainRegexComplexity, 0, minMainRegexLength, maxMainRegexLength);

            coreSubRegexes.add(coreSubRegex);
            numberOfCoreSubRegexes = 1;
        }else{

            int subIndex = 0;
            int subCount = 0;
            for(PublicRegexp sub : re.getSubs()){
                //System.out.println("sub " + sub+ " ");

                SubRegex.ComplexityLevel level = getRegexComplexity(sub);

                int minSubLength = PublicRegexp.computeMinLength(sub);
                int maxSubLength = PublicRegexp.computeMaxLength(sub);
               // System.out.println("sub " + sub+ " "+ minSubLength);

                // Keep also calculating the complexity of the full regex
                if(subIndex == 0){
                    mainRegexComplexity = level;
                }else{
                    if(level == SubRegex.ComplexityLevel.High){
                        mainRegexComplexity = SubRegex.ComplexityLevel.High;
                        // skip star and plus
                       // subCount ++;

                        //System.out.println("starsubCount " + subCount);
                        //continue;
                    }else if(level == SubRegex.ComplexityLevel.Medium &&
                            mainRegexComplexity != SubRegex.ComplexityLevel.High){
                        mainRegexComplexity = SubRegex.ComplexityLevel.Medium;
                    }
                }


                /*
                String subString = null;
                if(sub.toString().startsWith("("))
                    subString = sub.toString().substring(1, sub.toString().length()-1);
                else subString = sub.toString();
                */
                //System.out.println("subtoString " + sub.toString());
                RegexPredicate subRegexPredicate = new RegexPredicate(sub.toString(),
                        predicate.getAttributeNames(), predicate.isIgnoreCase(),
                        predicate.getSpanListName() + subIndex);
                SubRegex coreSubRegex = new SubRegex(subRegexPredicate, subIndex,1, level, subCount, minSubLength, maxSubLength);
                coreSubRegex.setOriginalSubCount(coreSubRegexes.size());
                coreSubRegexes.add(coreSubRegex);
                //System.out.println(coreSubRegex + "coreSubRegex " + coreSubRegex.originalSubId+" "+subIndex );


                // compute reverseSubRegex

                //String reverseSubString = "\\.([\\-0-9A-Z_a-z]+/)*[A-Za-z]?[A-Za-z]?[A-Za-z]?[A-Za-z]?[A-Za-z]?[A-Za-z][A-Za-z]\\.[\\--\\.0-9A-Za-z]+//:s?";

               // String reverseSub= "s?\\:\\/\\/[a-zA-Z0-9\\-\\.]+\\.[a-zA-Z]{2,7}(?:\\/[\\w\\-]+)*\\.)";
                String reverseSubString = PublicRegexp.reverseDeepCopy(sub);
               // System.out.println("ReverseString============== " + reverseSubString);

                RegexPredicate reverseSubRegexPredicate = new RegexPredicate(reverseSubString,
                        predicate.getAttributeNames(), predicate.isIgnoreCase(),
                        predicate.getSpanListName() + subIndex);
                SubRegex reverseSubRegex = new SubRegex(reverseSubRegexPredicate, subIndex,1, level, subCount, minSubLength,maxSubLength);
                coreSubRegex.setReverseSubRegex(reverseSubRegex);


                /*
                subRegexPattern = coreSubRegex.getsubRegexPredicate().isIgnoreCase() ?
                        Pattern.compile(coreSubRegex.getsubRegexPredicate().getRegex(), Pattern.CASE_INSENSITIVE)
                        : Pattern.compile(coreSubRegex.getsubRegexPredicate().getRegex());
                coreSubPatterns.add(subRegexPattern);

                if(level != SubRegex.ComplexityLevel.High) {
                    subRegexDirGraph.addVertex(coreSubRegex);
                    //System.out.println("coreSubRegex " + coreSubRegex);
                }

*/
               // System.out.println(sub + " coreSubRegex== ");
                //System.out.println("reverse " + reverseSubRegex);
                subIndex ++;
                subCount ++;
            }

            numberOfCoreSubRegexes = subIndex;
        }

        if(labels != null && ! labels.isEmpty()){
            mainRegex = null;
        }else{
            mainRegex = new SubRegex(predicate, 0, numberOfCoreSubRegexes, mainRegexComplexity, 1, minMainRegexLength, maxMainRegexLength);
            //SubRegex mainReverseRegex = new SubRegex(mainReverseRegexPredicate,
             //       0, numberOfCoreSubRegexes, mainRegexComplexity);
            //mainRegex.setReverseSubRegex(mainReverseRegex);

            if(coreSubRegexes.isEmpty()){
                coreSubRegexes.add(mainRegex);
                //coreSubRegexes.add(mainRegex);
            }
            mainRegex.setOriginalSubCount(numberOfCoreSubRegexes);
         }
         /*
        String startSubString = "^";
        RegexPredicate startSubRegexPredicate = new RegexPredicate(startSubString,
                predicate.getAttributeNames(), predicate.isIgnoreCase(),
                predicate.getSpanListName() + 0);
        startSubRegex = new SubRegex(startSubRegexPredicate, 0,1,  SubRegex.ComplexityLevel.Low);
        subRegexDirGraph.addVertex(startSubRegex);
        */
        // for A(B*C)
/*
        SubRegex nextSub = null;
        for (int i = 1; i < coreSubRegexes.size(); i++) {
            if(nextSub == null) {
                nextSub = coreSubRegexes.get(i);
            }
            else
                nextSub = combineSubs(nextSub, coreSubRegexes.get(i));
            //System.out.println("nextSub "+nextSub.regexPatern.toString());
        }
        coreSubRegexes.set(1, nextSub);

 */

/**
 * * test different order of subregex matching
 */

        coreSubRegexes.get(0).selectivity = 0.3;
        coreSubRegexes.get(1).selectivity = 0.2;
        coreSubRegexes.get(2).selectivity = 0.1;
        coreSubRegexes.sort(Comparator.comparing(SubRegex::getSelectivity));








//        System.out.println("---------------------------------------");
//        for(SubSequence sub: coreSubSequences){
//        	System.out.println(sub.toString());
//        }
//        System.out.println("---------------------------------------");

    }

    private SubRegex.ComplexityLevel getRegexComplexity(PublicRegexp re){
        SubRegex.ComplexityLevel level = SubRegex.ComplexityLevel.Low;
        if(PublicRegexp.hasOp(re, PublicRegexp.PublicOp.STAR) ||
                PublicRegexp.hasOp(re, PublicRegexp.PublicOp.PLUS) ||
                PublicRegexp.hasOp(re, PublicRegexp.PublicOp.QUEST)){
            level = SubRegex.ComplexityLevel.High;
            return level;
        }
        if(PublicRegexp.hasOp(re, PublicRegexp.PublicOp.ALTERNATE) ||
                PublicRegexp.hasOp(re, PublicRegexp.PublicOp.REPEAT) ||
                //PublicRegexp.hasOp(re, PublicRegexp.PublicOp.QUEST) ||
                PublicRegexp.hasOp(re, PublicRegexp.PublicOp.CAPTURE)){
            for(PublicRegexp sub : re.getSubs()){
                SubRegex.ComplexityLevel subLevel = getRegexComplexity(sub);
                //System.out.println(sub + " "+ subLevel);
                if(subLevel == SubRegex.ComplexityLevel.High) {

                    level = subLevel;
                    break;
                }
                else
                    level = SubRegex.ComplexityLevel.Medium;
            }

        }
        return level;
    }

    // Initializes the subRegexContainer structure with all possible expansions of
    // non-high sub-regexes by Jamshid

    private void generateExpandedSubRegexes(){

        // initialize the subRegex container
        for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
            subRegexContainer.add(new ArrayList<>());

        }

        // If it's high:
        // 				make the longest possible sub-regex starting from here ending before the next non-high sub-regex
        // If it's non-high:
        // 				just put the core sub-regex on that position and go to the next iteration
        // Note: Non-high and high sub-regexes are not mixed.

        for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){

            SubRegex coreSubRegex = coreSubRegexes.get(startingCSRIndex);
            RegexPredicate coreRegexPredicate = coreSubRegex.predicate;

            if(coreSubRegex.complexity != SubRegex.ComplexityLevel.High){
                if(coreSubRegex.isFirstSubSequence() && coreSubRegex.isLastSubSequence()){
                    // this is the full regex, we have previously prepared this regex in the break function
                    subRegexContainer.get(startingCSRIndex).add(mainRegex);
                    //subRegexGraph.addVertex(mainRegex);
                }else{
                    //System.out.println(startingCSRIndex + " -- " + coreSubRegex.toString());
                    subRegexContainer.get(startingCSRIndex).add(coreSubRegex);
                    //subRegexGraph.addVertex(coreSubRegex);

                }
                continue;
            }

            // High starting point
            RegexPredicate expandingPredicate = new RegexPredicate(coreRegexPredicate.getRegex(),
                    coreRegexPredicate.getAttributeNames(),
                    coreRegexPredicate.isIgnoreCase(),
                    coreRegexPredicate.getSpanListName()+startingCSRIndex);
            SubRegex.ComplexityLevel expandingComplexityLevel = coreSubRegex.complexity;
            RegexPredicate reverseExpandingPredicate = new RegexPredicate(coreSubRegex.getReverseSubRegex().predicate);

            int endingCSRIndex = startingCSRIndex;
            //System.out.println(startingCSRIndex+" endingCSRIndex " + endingCSRIndex);
            // expandingPredicate => R1 if R1 is ComplexityLevel.High
            for(; endingCSRIndex < coreSubRegexes.size(); ++endingCSRIndex){
               // System.out.println(startingCSRIndex + " endingCSRIndex " + endingCSRIndex);
                if(! coreSubRegexes.get(endingCSRIndex).isSubRegex()){
                    break;
                }
                SubRegex endingCoreSubRegex = coreSubRegexes.get(endingCSRIndex);
                if(endingCoreSubRegex.complexity != SubRegex.ComplexityLevel.High){
                    break;
                }

                if(startingCSRIndex != endingCSRIndex){
                    // expandingPredicate => R1R2
                    expandingPredicate.setRegex(expandingPredicate.getRegex() + endingCoreSubRegex.predicate.getRegex());
                    expandingPredicate.setSpanListName(expandingPredicate.getSpanListName() + endingCSRIndex);
                    //System.out.println("expandingPredicate" + expandingPredicate.getRegex());
                    // reverseExpandingPredicate => rev(R2)rev(R1)
                    reverseExpandingPredicate.setRegex(endingCoreSubRegex.getReverseSubRegex().predicate.getRegex() + reverseExpandingPredicate.getRegex());
                    reverseExpandingPredicate.setSpanListName(reverseExpandingPredicate.getSpanListName() + endingCSRIndex);
                    //System.out.println("reverseExpandingPredicate" + reverseExpandingPredicate.getRegex());
                }
            }
            //System.out.println("expandingPredicate" + expandingPredicate.getRegex());
            RegexPredicate newSubRegexPredicate = new RegexPredicate(expandingPredicate);
            SubRegex newSubRegex = new SubRegex(newSubRegexPredicate, startingCSRIndex, endingCSRIndex - startingCSRIndex, expandingComplexityLevel,0 ,0, 0);
            newSubRegex.setOriginalSubCount(coreSubRegexes.size());
            // Set the reverse sub regex of the new expanded subregex
            if(newSubRegex.isFirstSubSequence() && newSubRegex.isLastSubSequence()){
                // this is the full regex, we have previously prepared this regex in the break function
                subRegexContainer.get(startingCSRIndex).add(mainRegex);
                //subRegexGraph.addVertex(mainRegex);

            }else{
                RegexPredicate newReverseSubRegexPredicate = new RegexPredicate(reverseExpandingPredicate);
                SubRegex newReverseSubRegex = new SubRegex(newReverseSubRegexPredicate, startingCSRIndex, endingCSRIndex - startingCSRIndex, expandingComplexityLevel, 0, 0, 0);
                newSubRegex.setReverseSubRegex(newReverseSubRegex);
                subRegexContainer.get(startingCSRIndex).add(newSubRegex);
                //subRegexGraph.addVertex(newSubRegex);
                //System.out.println(startingCSRIndex + " -- " + newSubRegex.toString());
            }
            startingCSRIndex = endingCSRIndex - 1;
        }

        // Also add a sub-regex starting from zero with full length which would be the
        // original complete regex.

        if(mainRegex != null){
            // Check the longest sub-regex starting from zero
            SubRegex sub = subRegexContainer.get(0).get(subRegexContainer.get(0).size() - 1);
            if(! (sub.isFirstSubSequence() && sub.isLastSubSequence())){ // the regex was actually broken.
                // the large complete regex which starts from zero and goes to the end
                subRegexContainer.get(0).add(mainRegex);
            }
        }

        // Prepare query plan
        // print subRegexContainer
        /*
        for(int i=0;i<subRegexContainer.size();i++){
            for(int j=0; j< subRegexContainer.get(i).size(); j++)
                System.out.println(i + " -- " + j + " : " + subRegexContainer.get(i).get(j));
        }
        */
    }

    private void generateSubRegexesCotainStar(){
        SubRegex firstSub = coreSubRegexes.get(0);
        RegexPredicate fiestSubPredicate = firstSub.predicate;

        for(int i=1; i<coreSubRegexes.size();) {
            SubRegex coreSubRegex = coreSubRegexes.get(i);
            RegexPredicate coreRegexPredicate = coreSubRegex.predicate;
           // System.out.println("complex : " + coreSubRegex.complexity);
            if(coreSubRegex.complexity != SubRegex.ComplexityLevel.High){
                i++;
                continue;
            }else{
                RegexPredicate expandSubPredicate = coreSubRegexes.get(i-1).predicate;
                String expandSubString = expandSubPredicate.getRegex()+ coreRegexPredicate.getRegex();
                expandSubPredicate.setRegex(expandSubString);
                expandSubPredicate.setSpanListName(expandSubPredicate.getSpanListName());
                coreSubRegexes.remove(i);
               // System.out.println("expandingPredicate : " + expandSubPredicate.getRegex());

            }
            //System.out.println(startingCSRIndex+" endingCSRIndex " + endingCSRIndex);
            // expandingPredicate => R1 if R1 is ComplexityLevel.High

            //System.out.println("expandingPredicate" + expandingPredicate.getRegex());
        }

    }
    /**
     * by Yutong Han
     * intersection of two matching results for two sub-regexes, which R=AB A.end = B.start
     * @param matchingResults
     * @param currentResults
     * @return finalResults
     */
    private List<Span> computeSpanIntersection(List<Span> matchingResults, List<Span> currentResults){
        List<Span> finalResults = new ArrayList<>();

            // System.out.println("2 computeSpanIntersection: "+ currentResults.size()+matchingResults.size());
            for (int i  = 0; i < matchingResults.size(); i++) {
                for (int j  = 0; j < currentResults.size(); j++) {
                    Span span = matchingResults.get(i);
                    Span curSpan = currentResults.get(j);
                    if (span.getEnd() == curSpan.getStart()) {
                        String connectRegexPredicate = span.getKey() + curSpan.getKey();
                        int start = span.getStart();
                        int end = curSpan.getEnd();
                        String value = span.getValue()+curSpan.getValue();
                        finalResults.add( new Span(span.getAttributeName(), start, end, connectRegexPredicate, value));
                        //System.out.println("Results: "+start+"--"+end+" "+value);
                        break;
                    }

                }
            }
            return finalResults;


    }

    private List<Span> computeSpanIntersection(List<Span> matchingResults, List<Span> currentResults, SubRegex starSubRegex){
        List<Span> finalResults = new ArrayList<>();
        Pattern starPattern = starSubRegex.regexPatern;


        // System.out.println("2 computeSpanIntersection: "+ currentResults.size()+matchingResults.size());
        for (int i  = 0; i < matchingResults.size(); i++) {
            for (int j  = 0; j < currentResults.size(); j++) {
                Span span = matchingResults.get(i);
                Span curSpan = currentResults.get(j);
                if (span.getEnd() <= curSpan.getStart()) {
                    Matcher starMatcher = starPattern.matcher(fieldValue.substring(span.getEnd(), curSpan.getStart()));
                    if(starMatcher.matches()) {
                        String connectRegexPredicate = span.getKey() + starSubRegex.predicate.getRegex() + curSpan.getKey();
                        int start = span.getStart();
                        int end = curSpan.getEnd();
                        //String value = span.getValue() + curSpan.getValue();
                        finalResults.add(new Span("content", start, end, connectRegexPredicate, fieldValue.substring(start, end)));
                    }
                    //System.out.println("Results: "+start+"--"+end+" "+value);
                    else break;
                }

            }
        }
        return finalResults;


    }



    // find the minMax index before the position of next Span
    private int minMaxSearchMatchResult(List<Span> matchResults, int startSpan){
        int middleSpan = 0;
        int minSpan = 0;
        int maxSpan = matchResults.size()-1;

        while(minSpan <= maxSpan){
            //System.out.println( " range: "+ minSpan + "--" + maxSpan+ " "+ startSpan);
            //System.out.println( " min - max: "+ matchResults.get(minSpan).getStart() + "--" + matchResults.get(maxSpan).getStart());
            middleSpan = (minSpan + maxSpan) / 2;
            if(matchResults.get(middleSpan).getStart() > startSpan){
                maxSpan = middleSpan - 1;
            }else if(matchResults.get(middleSpan).getStart() < startSpan){
                minSpan = middleSpan + 1;
            }
            else return middleSpan;
        }
        if(matchResults.get(middleSpan).getStart() > startSpan)
            return  middleSpan-1;
        else return middleSpan;


    }


    /**
     * If matchSub is before currSub, gap > 0 , otherwise gap <=0.
     * @param matchSub
     * @param currSub
     * @return gap
     */
    private Pair<Integer, Integer> computeSubsOffset(SubRegex matchSub, SubRegex currSub){
        //System.out.println(currSub.startingSubSeqIndex + " --- " + matchSub.startingSubSeqIndex);
        int minOffset = 0;
        int maxOffset = 0;


        boolean idFlag = false;
        int idStart = 0;
        int idEnd = 0;
        int idOffset = currSub.startingSubSeqIndex - matchSub.startingSubSeqIndex;
        if(idOffset>0) {
            idStart = matchSub.startingSubSeqIndex;
            idEnd = currSub.startingSubSeqIndex;
            idFlag = true;
        }
        else {
            idStart = currSub.startingSubSeqIndex;
            idEnd = matchSub.startingSubSeqIndex;
        }

        if(Math.abs(idOffset)>1) {

            for(int i = idStart+1; i < idEnd; i++) {
                //System.out.println("i : " + i);
                minOffset = coreSubRegexes.get(i).minLength;
                if(idFlag) {
                   // System.out.println("flag : " + idFlag);
                    if(coreSubRegexes.get(i).complexity != SubRegex.ComplexityLevel.High) {
                        //System.out.println("subOffset : " + subOffset);
                        minOffset += coreSubRegexes.get(i).minLength;
                        maxOffset += coreSubRegexes.get(i).maxlength;
                    }
                    else {
                        minOffset = -1;
                        maxOffset = -1;
                    }
                }


            }
        }
        else {
            minOffset = coreSubRegexes.get(idStart).minLength;
            maxOffset = coreSubRegexes.get(idStart).maxlength;
        }
        Pair<Integer, Integer> subOffset = new MutablePair<>(minOffset,maxOffset);
        return subOffset;
    }
    private SubRegex combineSubs(SubRegex startSub, SubRegex endSub){
        String coSubString = startSub.predicate.getRegex().concat(endSub.predicate.getRegex());
        RegexPredicate coSubPredicate = new RegexPredicate(coSubString,
                predicate.getAttributeNames(), predicate.isIgnoreCase(),
                predicate.getSpanListName());
        SubRegex.ComplexityLevel level = SubRegex.ComplexityLevel.Low;
        int minRegexLength = startSub.minLength + endSub.minLength;
        int macRegexLength = startSub.maxlength + endSub.maxlength;
        if(startSub.complexity == SubRegex.ComplexityLevel.Medium || endSub.complexity == SubRegex.ComplexityLevel.Medium)
            level = SubRegex.ComplexityLevel.Medium;
        SubRegex coSubRegex = new SubRegex(coSubPredicate, startSub.startingSubSeqIndex,
                startSub.numberOfSubSequences, level, startSub.originalSubId, minRegexLength, macRegexLength);
        return  coSubRegex;
    }
    private void initializeOffsetMatrix(){
        minLengthMatrix = new int[coreSubRegexes.size()][coreSubRegexes.size()];
        maxLengthMatrix = new int[coreSubRegexes.size()][coreSubRegexes.size()];
        for(int i=0; i<coreSubRegexes.size(); i++){
            for(int j=i; j<coreSubRegexes.size();j++){
                SubRegex sub1 = coreSubRegexes.get(i);
                SubRegex sub2 = coreSubRegexes.get(j);
                //System.out.println(i + " "+ j);
                //System.out.println(sub1.originalSubId + " "+ sub2.originalSubId);
                if(i == j) {
                    minLengthMatrix[sub1.getOriginalSubSeqsCount()][sub2.startingSubSeqIndex] = 0;
                    maxLengthMatrix[sub1.getOriginalSubSeqsCount()][sub2.startingSubSeqIndex] = 0;
                }
                else {
                    if (i < sub1.originalSubId && j < sub2.originalSubId) {
                        if (i != j && j - i == sub2.originalSubId - sub1.originalSubId) {
                            minLengthMatrix[i][j] = computeSubsOffset(sub1, sub2).getLeft();
                            maxLengthMatrix[i][j] = computeSubsOffset(sub1, sub2).getRight();
                        }
                        else if (j - i < sub2.originalSubId - sub1.originalSubId)
                            minLengthMatrix[i][j] = -1;
                        else ;

                    } else if (i == sub1.originalSubId && j < sub2.originalSubId)
                        minLengthMatrix[i][j] = -1;
                    else {
                        minLengthMatrix[i][j] = computeSubsOffset(sub1, sub2).getLeft();
                        maxLengthMatrix[i][j] = computeSubsOffset(sub1, sub2).getRight();
                    }
                }
                //System.out.println("min" + minLengthMatrix[i][j]);
                //System.out.println("mmax" + maxLengthMatrix[i][j]);
            }
        }
    }
    private void printAllSubRegexes() {
        System.out.println("Core SubRegexes:");
        for (SubRegex core : coreSubRegexes) {
            System.out.println(core.toString()+" "+ core.getSelectivity() + "  " + core.originalSubId );
            if (core.isSubRegex()) {
               // System.out.println(((SubRegex) core).getReverseSubRegex().toString());
            }
        }
    }
    private void initializeSRG( TreeMap<Pair<Double, Double>, Pair<SubRegex, Integer>> subMatchMap) {
        //Double minMatchTime = 0.0;
        int count = 0;
        List<SubRegex> minSubList = new ArrayList<>();
        for (Map.Entry<Pair<Double, Double>, Pair<SubRegex, Integer>> subMatch : subMatchMap.entrySet()) {

            Double minSelectivity = subMatch.getKey().getLeft();

            if (minSelectivity == 0.0) {
                SubRegex minSub = subMatch.getValue().getLeft();
                minSubList.add(minSub);
                count++;
            }
            else {
                if(count == 0){
                    minSubList.add(subMatch.getValue().getLeft());
                    break;
                }
            }
        }
        for(SubRegex minSub : minSubList) {
            // addEdge about min selectivity with the min time costas
            //System.out.println("1minSub " + minSub);
            if (subRegexDirGraph.hasEdge(startSubRegex, minSub))
                //Update the weight on the edge
                subRegexDirGraph.updateEdgeWeight(startSubRegex, minSub, 1.0);
            else
                subRegexDirGraph.addEdge(startSubRegex, minSub, 1.0);

            for (Map.Entry<Pair<Double, Double>, Pair<SubRegex, Integer>> subMatch : subMatchMap.entrySet()) {

                Pair<SubRegex, Integer> matchMapSub = subMatch.getValue();
                SubRegex sub = matchMapSub.getLeft();

                int matchSize = matchMapSub.getRight();

                if (sub != minSub && sub.complexity != SubRegex.ComplexityLevel.High) {
                    // System.out.println("2start " + sub + "===== end " + minSub);
                    if (subRegexDirGraph.hasEdge(sub, minSub))
                        //Update the weight on the edge
                        subRegexDirGraph.updateEdgeWeight(sub, minSub, 1.0);
                    else
                        subRegexDirGraph.addEdge(sub, minSub, 1.0);
                }
            }
        }
        //System.out.println("SRG NumberOfEdges " + subRegexDirGraph.getNumberOfEdges());
        //System.out.println("SRG NumberOfNodes " + subRegexDirGraph.getNumberOfVertices());
    }
    private List<Span> computeMatchingSpans(String fieldValue, GraphInterface<SubRegex> subRegexDirGraph, List<SubRegex> nextSubList){
        List<Span> resultSpans = new ArrayList<>();
        List<SubRegex> resultSubList = new ArrayList<>();
        String subStringFieldValue = new String();


        for(int i=1; i<resultSpans.size(); i++) {
            int start = resultSpans.get(i).getStart();
            int end = resultSpans.get(i).getEnd();
            SubRegex frontSub = nextSubList.get(i-1);

            resultSubList = subRegexDirGraph.getMinNextVertexDFS(nextSubList);
            if (resultSubList.isEmpty()) ;
            else {
                SubRegex nextSub = resultSubList.get(resultSubList.size() - 1);
                if (nextSub.startingSubSeqIndex < frontSub.startingSubSeqIndex)
                    subStringFieldValue = fieldValue.substring(0, start);
                else
                    subStringFieldValue = fieldValue.substring(start, fieldValue.length() - 1);
                Pattern subPattern1 = Pattern.compile(nextSub.predicate.getRegex());
                Matcher javaMatcher = subPattern1.matcher(subStringFieldValue);

            }
        }
        return resultSpans;
    }
}
