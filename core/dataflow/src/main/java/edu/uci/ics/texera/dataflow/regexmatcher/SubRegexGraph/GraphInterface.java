package edu.uci.ics.texera.dataflow.regexmatcher.SubRegexGraph;

import java.util.List;
import java.util.Queue;

public interface GraphInterface<T> extends BasicGraphInterface<T>, GraphAlgorithmsInterface<T>{
     void updateEdgeWeight(T begin, T end, double weight);
    List<T> getMinNextVertexDFS(List<T> originList);
     void resetVertices();
}
