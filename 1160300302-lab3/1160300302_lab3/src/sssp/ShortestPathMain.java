package sssp;

import aggregator.ActiveVertexNumberAggregator;
import master.Master;

public class ShortestPathMain {

  public static void main(String[] args) {
    Master<Integer, Integer> master = new Master<Integer, Integer>();
    ShortestPathVertex test = new ShortestPathVertex("Single-Source-Shortest-Path-Test", "0");
    
    master.setCombiner(new ShortestPathCombiner());
    master.setAggregator(new ActiveVertexNumberAggregator(false));
    master.run(test);
    master.result("output/sssp/sssp_result.txt");
  }

}
