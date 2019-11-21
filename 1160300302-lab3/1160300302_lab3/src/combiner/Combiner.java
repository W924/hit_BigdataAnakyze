package combiner;

import java.util.List;

/**
 * Merge messages as they enter the send queue and the message enters the accept queue.
 * User needs to inherit this class when using it, and rewrites the combine() function.
 *
 * @param <L>, L is the type of messages.
 */
public interface Combiner<L> {

  /**
   * Merge the messages in List<L> messages.
   * 
   * @param messages, the list of received or sent messages.
   * @return List<L>, the merged message queue
   */
  public List<L> combine(List<L> messages);
  
}
