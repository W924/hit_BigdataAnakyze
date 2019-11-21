package vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import aggregator.Aggregator;

/**
 * The superclass of all Vertex which has basic properties such as 
 * identifier(String), user-defined value, a set of edges adjacent to the vertex, 
 * the state(active or inactive), sent-messages from this vertex and the vertex current superStep number.
 * 
 * @param <V>, V is the type of user-defined value.
 * @param <L>, L is the type of messages.
 */
public class Vertex<V, L> {

  private String id;
  private V value;
  private Set<String> edges;
  private boolean flag;
  
  private Map<String, List<L>> send_messages;
  private int superstep;
  
  private boolean aggregator_flag;
  private Aggregator aggregator;
  
  
  public Vertex(String id) {
    this.id = id;
    flag = true;
    edges = new HashSet<String>();
    send_messages = new HashMap<String, List<L>>();
    superstep = 0;
    aggregator_flag = false;
  }
  
  /**
   * Defines an active vertex in a SuperStep what to do in a superStep.
   * All subclass of Vertex should rewrite this method.
   * 
   * @param vertex, which execute the actions.
   * @param messages, the message received by the vertex.
   */
  public void compute(Vertex<V, L> vertex, List<L> messages) {
    
  }
  
  /**
   * Open the function of Aggregator.
   * 
   * @param aggregator, an instance of Aggregator.
   */
  public void setAggregator(Aggregator aggregator) {
    this.aggregator_flag = true;
    this.aggregator = aggregator;
  }

  /**
   * Get the identifier of the vertex.
   * 
   * @return id, id can not be null.
   */
  public String getID() {
    return id;
  }
  
  /**
   * Get the user-defined value of the vertex.
   * 
   * @return value, the user-defined value.
   */
  public V getValue() {
    return value;
  }
  
  /**
   * Get all edges adjacent to the vertex.
   * 
   * @return the set of edges adjacent to the vertex.
   */
  public Set<String> getEdges() {
    return edges;
  }
  
  /**
   * Get the state is active or not.
   * 
   * @return true if the state if active; false if the state is inactive.
   */
  public boolean getFlag() {
    return flag;
  }
  
  /**
   * Get the current superStep number of the vertex.
   * 
   * @return the number of current superStep.
   */
  public int getSuperStep() {
    return superstep;
  }
  
  /**
   * Get the state of aggregate function is open or not.
   * 
   * @return true if the aggregate function is open; else false;
   */
  public boolean getAggregatorFlag() {
    return aggregator_flag;
  }
  
  /**
   * 
   * 
   * @return the aggregator on the vertex if this function is open; else null.
   */
  public Aggregator getAggregator() {
    return aggregator;
  }
  
  /**
   * Get the message sent from this vertex.
   * 
   * @return a Map<String, List<L>> type of sent messages from this vertex.
   */
  public Map<String, List<L>> getSendMessages() {
    return send_messages;
  }
  
  /**
   * Clear the send message queue of this vertex.
   * 
   */
  public void clearSendMessage() {
    this.send_messages = new HashMap<String, List<L>>();
  }
  
  /**
   * Modify user-defined value of vertex.
   * 
   * @param value, can not be null.
   */
  public void mutableValue(V value) {
    this.value = value;
  }
  
  /**
   * Set the edges adjacent to the vertex.
   * 
   * @param vertices, the edges use Set<String> to indicate.
   */
  public void setEdges(Set<String> vertices) {
    edges.addAll(vertices);
  }
  
  /**
   * Change the state to inactive.
   * 
   */
  public void voltToHalt() {
    this.flag = false;
  }
  
  /**
   * change the state to active.
   * 
   */
  public void voltToActive() {
    this.flag = true;
  }
  
  /**
   * Increase the number of superStep of the vertex.
   * 
   */
  public void superStepInc() {
    superstep++;
  }
  
  /**
   * Add messages from this vertex to the worker's send queue.
   * 
   * @param dest_vertex, can not be null.
   * @param message, 
   */
  public void sendMessageTo(String dest_vertex, L message) {
    List<L> messages;
    if(send_messages.get(dest_vertex) == null) {
      messages = new ArrayList<L>();
    } else {
      messages = send_messages.get(dest_vertex);
    }
    messages.add(message);
    send_messages.put(dest_vertex, messages);
  }
  
  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + id.hashCode();
    return result;
  }
  
  @Override
  public boolean equals(Object that) {
    if(this == that) {
      return true;
    }
    if(that == null) {
      return false;
    }
    if(that instanceof Vertex) {
      Vertex<?, ?> other = (Vertex<?, ?>)that;
      return other.getID().equals(this.getID());
    }
    return false;
  }
  
  @Override
  public String toString() {
    return id;
  }
  
}