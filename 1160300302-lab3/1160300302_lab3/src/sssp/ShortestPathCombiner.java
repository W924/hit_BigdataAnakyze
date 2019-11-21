package sssp;

import java.util.ArrayList;
import java.util.List;
import combiner.Combiner;

/**
 * The Combiner of Shortest Path.
 *
 */
public class ShortestPathCombiner implements Combiner<Integer> {

  /**
   * returns the queue which retains only the minimum value in messages.
   * 
   */
  @Override
  public List<Integer> combine(List<Integer> messages) {
    int mindist = Integer.MAX_VALUE;
    for(int i=0; i<messages.size(); i++) {
      mindist = Math.min(mindist, messages.get(i));
    }
    
    messages = new ArrayList<Integer>();
    messages.add(mindist);
    return messages;
  }

}
