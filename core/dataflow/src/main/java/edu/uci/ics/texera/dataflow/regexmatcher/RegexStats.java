package edu.uci.ics.texera.dataflow.regexmatcher;

import edu.uci.ics.texera.api.span.Span;

import java.util.ArrayList;
import java.util.List;
class SpanListSummary{
    public int startMin;
    public int startMax;
    public int endMin;
    public int endMax;
    public float lengthAverage;
    public int size;
    public SpanListSummary(int sMin, int sMax, int eMin, int eMax, float lAvg, int s){
        startMin = sMin;
        startMax = sMax;
        endMin = eMin;
        endMax = eMax;
        lengthAverage = lAvg;
        size = s;
    }
    public SpanListSummary(){
        startMin = -1;
        startMax = -1;
        endMin = -1;
        endMax = -1;
        lengthAverage = -1;
        size = 0;
    }
    // assumes all attributes are the same in the spans of the spanList
    public static SpanListSummary summerize(List<Span> spanList){
        SpanListSummary summary = new SpanListSummary();
        for(Span s: spanList){
            summary.addSpan(s);
        }
        return summary;
    }
    private void addSpan(Span s){
        if(size == 0){
            size = 1;
            startMin = s.getStart();
            startMax = s.getStart();
            endMin = s.getEnd();
            endMax = s.getEnd();
            lengthAverage = s.getEnd() - s.getStart();
        }else{
            startMin = Math.min(startMin, s.getStart());
            startMax = Math.max(startMax, s.getStart());
            endMin = Math.min(endMin, s.getEnd());
            endMax = Math.max(endMax, s.getEnd());
            lengthAverage = (lengthAverage * size + (s.getEnd() - s.getStart())) / (size + 1);
            size ++;
        }
    }
}

public class RegexStats {

    int df = 0;
    List<Double> tfAveragePerAttribute = new ArrayList<>();

    int successDataPointCounter = 0;
    int failureDataPointCounter = 0;

    double successCostAverage = 0;
    double failureCostAverage = 0;

    double matchingSrcAvgLen = 0;

    public RegexStats(int numAttributes){
        for(int i = 0 ;  i < numAttributes; i++){
            tfAveragePerAttribute.add(0.0);
        }
        tfAveragePerAttribute.set(0, 1.0);
        // To avoid absolute zero for selectivity.
        successDataPointCounter++;
    }

    public void addStatsSubRegexFailure(double failureCost, int matchingSrcSize){
        if(failureCost != 0){ // Cost average won't be updated if the passed input is zero
            failureCostAverage = ((failureDataPointCounter * failureCostAverage) + failureCost ) / (failureDataPointCounter + 1);
        }
        failureDataPointCounter ++;
        addStatsMatchingSrcSize(matchingSrcSize);
    }

    public void addStatsSubRegexSuccess(double successCost, int matchingSrcSize){
        // update cost
        if(successCost != 0){ // Update cost average only if the passed input is greater than zero
            successCostAverage = ((successDataPointCounter * successCostAverage) + successCost ) / (successDataPointCounter + 1);
        }
        df ++;
        // update selectivity
        successDataPointCounter ++;
        // update matching src size
        addStatsMatchingSrcSize(matchingSrcSize);
    }

    public void addTfAveragePerAttribute(List<Integer> numberOfMatchSpans){
        if(successDataPointCounter == 0){
            System.out.println("Unexpected state. Adding tfAverage while success is not incremented."); // TODO: remove or replace with exception
            return;
        }
        successDataPointCounter --;
        // update the tf values
        if(numberOfMatchSpans.size() == tfAveragePerAttribute.size()){
            for(int i = 0 ; i < tfAveragePerAttribute.size(); ++i){
                tfAveragePerAttribute.set(i,
                        ((successDataPointCounter * tfAveragePerAttribute.get(i)) + numberOfMatchSpans.get(i)) /
                                (successDataPointCounter + 1) );
            }
        }else if(! numberOfMatchSpans.isEmpty()){
            System.out.println("tfAverage input not consistent with the number of attributes."); // TODO remove the print
        }
        successDataPointCounter ++;
    }

    private void addStatsMatchingSrcSize(int matchingSrcSize){
        matchingSrcAvgLen = (matchingSrcAvgLen * Math.max(getSize()-1,0)) + matchingSrcSize;
        matchingSrcAvgLen = matchingSrcAvgLen / Math.max(getSize(), 1);
    }

    public double getSelectivity(){
        double selectivity = (successDataPointCounter * 1.0 / getSize());
        if(selectivity == 0){
            return 0.001;
        }
        return selectivity;
    }

    public double getExpectedCost(){
        return (successCostAverage * getSelectivity()) + (failureCostAverage * (1 - getSelectivity()));
    }

    public int getSize(){
        return successDataPointCounter + failureDataPointCounter;
    }

    public List<Double> getTfAveragesPerAttributes(){
        return tfAveragePerAttribute;
    }

    public double getTotalTfAverages(){
        double totalTfAverage = 0;
        for(double tf : getTfAveragesPerAttributes()){
            totalTfAverage += tf;
        }
        return totalTfAverage;
    }

    public double getConfidenceValue(){
        return 2.58 * Math.sqrt(getSelectivity() * (1 - getSelectivity()) / getSize() );
    }
}
