package edu.uci.ics.texera.dataflow.regexmatcher;

import edu.uci.ics.texera.api.span.Span;

import java.util.List;

public abstract class AbstractSubSequence {
    // CSR = CoreSubRegex
    int startingSubSeqIndex;
    int numberOfSubSequences;
    int originalSubSeqsCount;
    // int originalSubCount;
    // spans belong to the matches in the latest tuple
    List<Span> latestMatchingSpans = null;


    // Structure for keeping statistics
    RegexStats stats = null;

    public AbstractSubSequence(int start, int length){
        startingSubSeqIndex = start;
        numberOfSubSequences = length;
    }

    public int getOriginalSubSeqsCount(){
        return originalSubSeqsCount;
    }
    public void setOriginalSubCount(int count){
        originalSubSeqsCount = count;
    }

    public int getStart(){
        return startingSubSeqIndex;
    }
    public int getEnd(){
        return startingSubSeqIndex + numberOfSubSequences;
    }
    public RegexStats getStats() {
        return stats;
    }

    public boolean isFirstSubSequence(){
        return getStart() == 0;
    }

    public boolean isLastSubSequence(){
        return getEnd() == getOriginalSubSeqsCount();
    }

    public List<Span> getLatestMatchingSpanList(){
        return latestMatchingSpans;
    }

    public void resetMatchingSpanList(List<Span> spanList){
        latestMatchingSpans = spanList;
    }

    public String toStringShort(){
        return "[" + getStart() + "," + getEnd() + ") ";
    }

    public abstract boolean isSubRegex();

    public abstract List<String> getAttributeNames();
}
class LabelSubSequence extends AbstractSubSequence{
    String labelName = "";
    List<String> attributeNames = null;
    public LabelSubSequence(int start, int length, String labelName, List<String> attrNames){
        super(start, length);
        this.labelName = labelName;
        this.attributeNames = attrNames;
        stats = new RegexStats(attributeNames.size());
    }

    public String toString(){
        return toStringShort() + "<<" + labelName + ">>";
    }

    @Override
    public boolean isSubRegex() {
        return false;
    }

    @Override
    public List<String> getAttributeNames() {
        return attributeNames;
    }
}
