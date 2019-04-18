package edu.uci.ics.texera.dataflow.regexmatcher;


//import com.google.re2j.Pattern;
import java.util.regex.*;

import java.util.List;

public class SubRegex extends AbstractSubSequence{
    public enum ComplexityLevel{
        High,
        Medium,
        Low
    }
    Pattern regexPatern;
    Pattern startWithRegexPattern;
    Pattern startToEndRegexPattern;
    Pattern endWithRegexPattern;
    RegexPredicate predicate;
    ComplexityLevel complexity;
    SubRegex reverseSubRegex;
    Double selectivity;
    int minLength;
    int maxlength;
    int originalSubId;

    public Double getSelectivity() {
        return selectivity;
    }

    public SubRegex(){
        super(-1, -1);
        super.setOriginalSubCount(-1);
        originalSubId = 0;
        regexPatern = null;
        startWithRegexPattern = null;
        startToEndRegexPattern = null;
        endWithRegexPattern = null;
        this.complexity = ComplexityLevel.High;
        reverseSubRegex = null;
        minLength = 0;
        maxlength = 0;
        selectivity = 0d;

    }


    public RegexPredicate getsubRegexPredicate() {
        return predicate;
    }

    public void setRegex(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    public void setSelectivity(Double selectivity) {
        this.selectivity = selectivity;
    }

    public SubRegex(RegexPredicate predicate, int startingCSRIndex, int numberOfCSRs, ComplexityLevel complexity, int  originalSubId, int minLength, int maxlength){
        super(startingCSRIndex, numberOfCSRs);
        super.setOriginalSubCount(0);
        regexPatern = regexPatern.compile(predicate.getRegex());
        startWithRegexPattern = startWithRegexPattern.compile("^" + predicate.getRegex());
        startToEndRegexPattern = startToEndRegexPattern.compile("^" + predicate.getRegex() + "$");
        endWithRegexPattern = endWithRegexPattern.compile(predicate.getRegex() + "$");
        this.predicate = predicate;
        this.complexity = complexity;
        this.reverseSubRegex = reverseSubRegex;
        this.minLength = minLength;
        this.maxlength = maxlength;
        stats = new RegexStats(this.predicate.getAttributeNames().size());
        this.originalSubId = originalSubId;
        selectivity = 0d;

    }

    public void setOriginalSubCount(int count){
        super.setOriginalSubCount(count);
        if(reverseSubRegex != null){
            reverseSubRegex.setOriginalSubCount(count);
        }
    }

    public void setReverseSubRegex(SubRegex reverse){
        reverseSubRegex = reverse;
    }

    public SubRegex getReverseSubRegex(){
        return reverseSubRegex;
    }

    public String toString(){
        return toStringShort() + "///" + complexity + "///" + predicate.getRegex();
    }

    public static String getPlanSignature(List<SubRegex> plan){
        String signature = "";
        for(SubRegex s: plan){
            signature += s.toStringShort();
        }
        return signature;
    }

    public boolean isReverseExecutionFaster(){
        // No reverse available or there is no stats collected for reverse
        if(getReverseSubRegex() == null || getReverseSubRegex().stats.getSize() == 0){
            return false;
        }
        return getReverseSubRegex().stats.getExpectedCost() < stats.getExpectedCost();
    }

    public double getExpectedCost(){
        if(isReverseExecutionFaster()){
            return getReverseSubRegex().stats.getExpectedCost();
        }else{
            return stats.getExpectedCost();
        }
    }

    @Override
    public boolean isSubRegex() {
        return true;
    }
    @Override
    public List<String> getAttributeNames() {
        return predicate.getAttributeNames();
    }
}
