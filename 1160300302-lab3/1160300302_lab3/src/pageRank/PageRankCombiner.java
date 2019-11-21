package pageRank;

import java.util.ArrayList;
import java.util.List;
import combiner.Combiner;

/**
 * The combiner of PageRank.
 *
 */
public class PageRankCombiner implements Combiner<Double> {

  /**
   * Returns the queue which retains only the sum of messages.
   * 
   */
  @Override
  public List<Double> combine(List<Double> messages) {
    double sum = 0;
    for(int i=0; i<messages.size(); i++) {
      sum += messages.get(i);
    }
    
    messages = new ArrayList<Double>();
    messages.add(sum);
    return messages;
  }

}
