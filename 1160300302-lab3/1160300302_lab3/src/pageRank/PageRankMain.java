package pageRank;

import aggregator.VertexNumberAggregator;
import master.Master;

public class PageRankMain {

  public static void main(String[] args) {
    Master<Double, Double> master = new Master<Double, Double>();
    PageRankVertex test = new PageRankVertex("PageRank-Test", 10);
    
    master.setCombiner(new PageRankCombiner());
    master.setAggregator(new VertexNumberAggregator(true));
    master.run(test);
    master.result("output/pagerank/pagerank_result.txt");
  }
  
}
