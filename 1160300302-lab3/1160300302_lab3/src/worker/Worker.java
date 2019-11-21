package worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import vertex.Vertex;
import java.util.Set;
import aggregator.Aggregator;
import combiner.Combiner;

/**
 * Worker node which maintains a partition of graph data.
 *
 * @param <V>, V is the type of user-defined value.
 * @param <L>, L is the type of messages.
 */
public class Worker<V, L> {

  private String id;
  
  private Set<Vertex<V, L>> vertices;
  
  private Map<String, List<L>> receive_messages;
  private Map<String, List<L>> send_messages;
  private int superstep;
  
  private Map<String, Vertex<V, L>> id_vertex_map;
  
  private boolean combiner_flag;
  private Combiner<L> combiner;
  
  private boolean aggregator_flag;
  private Aggregator aggregator;
  
  
  public Worker(String id) {
    this.id = id;
    vertices = new HashSet<Vertex<V, L>>();
    receive_messages = new HashMap<String, List<L>>();
    send_messages = new HashMap<String, List<L>>();
    superstep = 0;
    combiner_flag = false;
    aggregator_flag = false;
  }
  
  /**
   * Traverse all vertices, let active vertices execute their compute(),
   * then get the messages sent by the vertex. If the combiner function is opened,
   * when the message enters the receive queue, use combine() rewrote in combiner to merge the messages.
   * 
   * @param vertex, the vertex to execute compute().
   */
  public void run(Vertex<V, L> vertex) {
    aggregator.initValue();
    initSendMessage();
    for(Vertex<V, L> v:vertices) {
      if(v.getFlag() == true) {
        List<L> receive_message = new ArrayList<L>();
        if(receive_messages.get(v.getID()) != null) {
          receive_message = receive_messages.get(v.getID());
        }
        vertex.compute(v, receive_message);
        
        if(aggregator_flag && aggregator.isAutoClosed() && superstep == 0) {
          aggregator.aggregate(aggregator.report(vertex));
        } else if(aggregator_flag && !aggregator.isAutoClosed()) {
          aggregator.aggregate(aggregator.report(vertex));
        }
        
        Map<String, List<L>> send_message = v.getSendMessages();
        for(Entry<String, List<L>> entry : send_message.entrySet()) {
          String tmp_vertex = entry.getKey();
          List<L> tmp_message = entry.getValue();
          if(send_messages.containsKey(tmp_vertex)) {
            send_messages.get(tmp_vertex).addAll(tmp_message);
          } else {
            send_messages.put(tmp_vertex, tmp_message);
          }
          
          if(combiner_flag) {
            send_messages.put(tmp_vertex, combiner.combine(send_messages.get(tmp_vertex)));
          }
        }
      }
      v.clearSendMessage();
      v.superStepInc();
    }
    initReceiveMessage();
  }
  
  /**
   * Load a partition of graph.
   * And build the Mapping relation between vertex'id and vertex.
   * 
   * @param fileNumber, a partition of graph data.
   */
  public void load(String fileNumber) {
    Map<String, Set<String>> edge_map = new HashMap<String, Set<String>>();
    
    File file = new File("input/" + fileNumber + "vertex.txt");
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      String line_read = null;
      String source_vertex;
      String dest_vertex;
      while((line_read = reader.readLine()) != null) {
        source_vertex = line_read;
        edge_map.put(source_vertex, new HashSet<String>());
      }
      reader.close();
      
      file = new File("input/" + fileNumber + "edge.txt");
      reader = new BufferedReader(new FileReader(file));
      while((line_read = reader.readLine()) != null) {
        source_vertex = line_read.split("\t")[0];
        dest_vertex = line_read.split("\t")[1];
        edge_map.get(source_vertex).add(dest_vertex);
      }
      reader.close();
      for(Entry<String, Set<String>> entry:edge_map.entrySet()) {
        Vertex<V, L> vertex = new Vertex<V, L>(entry.getKey());
        vertex.setEdges(entry.getValue());
        vertices.add(vertex);
      }
      System.out.println("worker" + fileNumber + " load completely.");
    } catch(IOException e) {
      e.printStackTrace();
    } finally {
      if(reader != null) {
        try {
          reader.close();
        } catch(IOException e1) {
          e1.printStackTrace();
        }
      }
    }
    
    id_vertex_map = new HashMap<String, Vertex<V, L>>();
    for(Vertex<V, L> v:vertices) {
      id_vertex_map.put(v.getID(), v);
    }
  }
  
  /**
   * Determine if the worker is over.
   * 
   * @return true if there is no message to send and all the vertex in the worker is in inactive;
   *         false if not.
   */
  public boolean isEnd() {
    boolean flag = true;
    if(send_messages.size() > 0) {
      flag = false;
    }
    for(Vertex<V, L> v:vertices) {
      if(v.getFlag() == true) {
        flag = false;
      }
    }
    return flag;
  }
  
  /**
   * Turn on the combiner function, and afferent a combiner.
   * 
   * @param combiner, an implement of combiner, in which the combine() is rewrote.
   */
  public void setCombiner(Combiner<L> combiner) {
    combiner_flag = true;
    this.combiner = combiner;
  }
  
  /**
   * Turn on the aggregate function.
   * 
   * @param aggregator
   */
  public void setAggregator(Aggregator aggregator) {
    aggregator_flag = true;
    this.aggregator = aggregator;
  }
  
  /**
   * Clear the receive messages queue of this worker.
   * 
   */
  public void initReceiveMessage() {
    this.receive_messages = new HashMap<String, List<L>>();
  }
  
  /**
   * Clear the send messages queue of this worker.
   * 
   */
  public void initSendMessage() {
    this.send_messages = new HashMap<String, List<L>>();
  }
  
  /**
   * Get the id of worker.
   * 
   */
  public String getID() {
    return id;
  }
  
  /**
   * Get the vertices in this worker.
   * 
   * @return Set<Vertex<V, L>>, the set of vertices.
   */
  public Set<Vertex<V, L>> getVertices() {
    return vertices;
  }
  
  /**
   * Get the messages sent from this worker.
   * 
   * @return Map<String, List<L>>, the key is the destination vertex,
   *            the value is the messages sent to the destination vertex.
   */
  public Map<String, List<L>> getSendMessages() {
    return send_messages;
  }
  
  /**
   * Get the number of vertices.
   * 
   * @return the number of vertices.
   */
  public int getNumberOfVertices() {
    return vertices.size();
  }
  
  /**
   * Get the number of edges.
   * 
   * @return the number of edges.
   */
  public int getNumberofEdges() {
    int sum = 0;
    for(Vertex<V, L> v:vertices) {
      sum += v.getEdges().size();
    }
    return sum;
  }
  
  public Aggregator getAggregator() {
    return aggregator;
  }
  
  /**
   * INC superStep number of worker.
   * 
   */
  public void superStepINC() {
    superstep++;
  }
  
  /**
   * Determine the worker contains vertex which has designated id or not.
   * 
   * @param vertex_id, the designated id, can not be null.
   * @return true if the worker has the vertex; else not.
   */
  public boolean contain(String vertex_id) {
    return id_vertex_map.containsKey(vertex_id);
  }
  
  /**
   * Receive messages sent to the vertex on this worker.
   * If the combiner is opened, when the message enters the receive queue, 
   * use combine() rewrote in combiner to merge the messages.
   * 
   * @param dest_vertex, the worker must contain this vertex.
   * @param message, a list of message.
   */
  public void receiveMessage(String dest_vertex, List<L> message) {
    if(receive_messages.containsKey(dest_vertex)) {
      receive_messages.get(dest_vertex).addAll(message);
    } else {
      receive_messages.put(dest_vertex, message);
    }
    
    id_vertex_map.get(dest_vertex).voltToActive();
    
    if(combiner_flag) {
      receive_messages.put(dest_vertex, combiner.combine(receive_messages.get(dest_vertex)));
    }
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
    if(that instanceof Worker) {
      Worker<?, ?> other = (Worker<?, ?>)that;
      return other.getID().equals(this.id);
    }
    return false;
  }
  
  @Override
  public String toString() {
    return id;
  }

}
