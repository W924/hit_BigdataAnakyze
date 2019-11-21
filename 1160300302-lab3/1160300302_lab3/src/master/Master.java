package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import aggregator.Aggregator;
import combiner.Combiner;
import statistics.Statistics;
import vertex.Vertex;
import worker.Worker;

/**
 * Master node which maintains all the workers.
 * Issue execution to all workers, start the corresponding operation.
 * Maintain statistics for the calculation process.
 *
 * @param <V>, V is the type of user-defined value.
 * @param <L>, L is the type of messages.
 */
public class Master<V, L> {

  private List<Worker<V, L>> workers;
  private Statistics statistics;
  
  private boolean aggregator_flag;
  private Aggregator aggregator;
  
  public Master() {
    workers = new ArrayList<Worker<V, L>>();
    statistics = new Statistics();
    aggregator_flag = false;
    initial();
  }
  
  /**
   * Turn on the combiner function.
   * 
   * @param combiner, an instance of Combiner.
   */
  public void setCombiner(Combiner<L> combiner) {
    for(Worker<V, L> worker:workers) {
      worker.setCombiner(combiner);
    }
  }
  
  /**
   * Turn on the aggregator function.
   * 
   * @param aggregator, an instance of Aggregator.
   */
  public void setAggregator(Aggregator aggregator) {
    aggregator_flag = true;
    this.aggregator = aggregator;
    for(Worker<V, L> worker:workers) {
      worker.setAggregator(aggregator.getInstance(aggregator.isAutoClosed()));
    }
  }

  /**
   * Execute the BSP calculation mode in Pregel.
   * In each SuperStep, all the workers run compute() on the vertices they are responsible for.
   * Then send messages. If there is no message to send and all the vertices are inactive, then end.
   * 
   * @param vertex, user-rewrote subclass of Vertex, use the rewrote compute() to 
   *            complete the calculation of active vertices.
   */
  public void run(Vertex<V, L> vertex) {
    int superstep = 0;
    while(true) {
      System.out.println("------------------------superstep: " + superstep + "---------------------------");
      System.out.println("superstep: " + superstep + " running…………");
      if(aggregator_flag && !aggregator.isAutoClosed()) {
        aggregator.initValue();
      }
      
      long startTime = System.nanoTime();
      for(Worker<V, L> worker:workers) {
        long tmp_startTime = System.nanoTime();
        worker.run(vertex);
        long tmp_endTime = System.nanoTime();
        System.out.println(worker + " running time: " + (tmp_endTime - tmp_startTime) / Math.pow(10, 9) + "s");
        statistics.addComputeTime(superstep, worker.getID(), (tmp_endTime - tmp_startTime) / Math.pow(10, 9));
        worker.superStepINC();
        
        if(aggregator_flag && aggregator.isAutoClosed() && superstep == 0) {
          aggregator.aggregate(worker.getAggregator().getAggregateValue());
        } else if(aggregator_flag && !aggregator.isAutoClosed()) {
          aggregator.aggregate(worker.getAggregator().getAggregateValue());
        }
      }
      long endTime = System.nanoTime();
      
      System.out.println("superstep: " + superstep + " run complete, time: " + (endTime - startTime) / Math.pow(10, 9) + "s");
      
      if(isEnd()) {
        System.out.println("END");
        break;
      } else {
        System.out.println("superstep: " + superstep + " communicating…………");
        communication(superstep);
      }
      // 如果Aggregator默认一直开启的话，每轮SuperStep都要输出当前的Aggregate Value.
      if(aggregator_flag && !aggregator.isAutoClosed()) {
        System.out.println("superstep: " + superstep + " Aggregate Value: " + aggregator.getAggregateValue());
      }
      superstep++;
    }
    
    // 如果Aggregator默认一轮superStep后自动关闭的话，仅在BSP过程结束后输出全局的Aggregate Value.
    if(aggregator_flag && aggregator.isAutoClosed()) {
      System.out.println("Aggregate Value: " + aggregator.getAggregateValue());
    }
  }

  /**
   * Initial all the worker. Load partitions of graph data to worker.
   * 
   */
  public void initial() {
    Worker<V, L> worker1 = new Worker<V, L>("worker1");
    Worker<V, L> worker2 = new Worker<V, L>("worker2");
    Worker<V, L> worker3 = new Worker<V, L>("worker3");
    Worker<V, L> worker4 = new Worker<V, L>("worker4");
    Worker<V, L> worker5 = new Worker<V, L>("worker5");
    Worker<V, L> worker6 = new Worker<V, L>("worker6");
    Worker<V, L> worker7 = new Worker<V, L>("worker7");
    Worker<V, L> worker8 = new Worker<V, L>("worker8");
    workers.add(worker1);
    workers.add(worker2);
    workers.add(worker3);
    workers.add(worker4);
    workers.add(worker5);
    workers.add(worker6);
    workers.add(worker7);
    workers.add(worker8);
    worker1.load("1");
    System.out.println(worker1 + " vertex number: " + worker1.getNumberOfVertices() + ", edge number: " + worker1.getNumberofEdges());
    statistics.addVertexNumber(worker1.getID(), worker1.getNumberOfVertices());
    statistics.addEdgeNumber(worker1.getID(), worker1.getNumberofEdges());
    worker2.load("2");
    System.out.println(worker2 + " vertex number: " + worker2.getNumberOfVertices() + ", edge number: " + worker2.getNumberofEdges());
    statistics.addVertexNumber(worker2.getID(), worker2.getNumberOfVertices());
    statistics.addEdgeNumber(worker2.getID(), worker2.getNumberofEdges());
    worker3.load("3");
    System.out.println(worker3 + " vertex number: " + worker3.getNumberOfVertices() + ", edge number: " + worker3.getNumberofEdges());
    statistics.addVertexNumber(worker3.getID(), worker3.getNumberOfVertices());
    statistics.addEdgeNumber(worker3.getID(), worker3.getNumberofEdges());
    worker4.load("4");
    System.out.println(worker4 + " vertex number: " + worker4.getNumberOfVertices() + ", edge number: " + worker4.getNumberofEdges());
    statistics.addVertexNumber(worker4.getID(), worker4.getNumberOfVertices());
    statistics.addEdgeNumber(worker4.getID(), worker4.getNumberofEdges());
    worker5.load("5");
    System.out.println(worker5 + " vertex number: " + worker5.getNumberOfVertices() + ", edge number: " + worker5.getNumberofEdges());
    statistics.addVertexNumber(worker5.getID(), worker5.getNumberOfVertices());
    statistics.addEdgeNumber(worker5.getID(), worker5.getNumberofEdges());
    worker6.load("6");
    System.out.println(worker6 + " vertex number: " + worker6.getNumberOfVertices() + ", edge number: " + worker6.getNumberofEdges());
    statistics.addVertexNumber(worker6.getID(), worker6.getNumberOfVertices());
    statistics.addEdgeNumber(worker6.getID(), worker6.getNumberofEdges());
    worker7.load("7");
    System.out.println(worker7 + " vertex number: " + worker7.getNumberOfVertices() + ", edge number: " + worker7.getNumberofEdges());
    statistics.addVertexNumber(worker7.getID(), worker7.getNumberOfVertices());
    statistics.addEdgeNumber(worker7.getID(), worker7.getNumberofEdges());
    worker8.load("8");
    System.out.println(worker8 + " vertex number: " + worker8.getNumberOfVertices() + ", edge number: " + worker8.getNumberofEdges());
    statistics.addVertexNumber(worker8.getID(), worker8.getNumberOfVertices());
    statistics.addEdgeNumber(worker8.getID(), worker8.getNumberofEdges());
  }
  
  /**
   * Communication.
   * The combine function is not here, it is in Worker.When a worker receive a message
   * or a message arrives at worker's send queue, the combine() works.
   * 
   * @param superstep, the number of superStep of the current process.
   */
  public void communication(int superstep) {
    String dest_vertex;
    List<L> message;
    int sum = 0;
    
    for(Worker<V, L> worker:workers) {
      int tmp_sum = 0;
      Map<String, List<L>> send_messages = worker.getSendMessages();
      for(Entry<String, List<L>> entry:send_messages.entrySet()) {
        dest_vertex = entry.getKey();
        message = entry.getValue();
        sum += message.size();
        tmp_sum += message.size();
        for(Worker<V, L> w:workers) {
          if(w.contain(dest_vertex)) {
            w.receiveMessage(dest_vertex, message);
            break;
          }
        }
      }
      System.out.println(worker + " communication size: " + tmp_sum);
      statistics.addCommunicationNumber(superstep, worker.getID(), tmp_sum);
    }
    
    System.out.println("total communication size: " + sum);
    System.out.println("superstep: " + superstep + " communication complete");
  }
  
  /**
   * Determine the BSP calculation process is end or not.
   * 
   * @return true if there is no message to send and all the vertices are inactive.
   *         false if not.
   */
  public boolean isEnd() {
    boolean flag = true;
    for(Worker<V, L> worker:workers) {
      if(!worker.isEnd()) {
        flag = false;
      }
    }
    return flag;
  }
  
  /**
   * The edge-cut random algorithm. Partition the graph to partitions.
   * 
   * @param filePath, the original graph data location.
   */
  public void partition(String filePath) {
    Map<Integer, Set<String>> vertices_map = new HashMap<Integer, Set<String>>();
    Map<Integer, Set<String>> edges_map = new HashMap<Integer, Set<String>>();
    for(int i=1; i<=8; i++) {
      vertices_map.put(i, new HashSet<String>());
      edges_map.put(i, new HashSet<String>());
    }
    File file = new File(filePath);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      String line_read = null;
      while((line_read = reader.readLine()) != null) {
        String source = line_read.split("\t")[0];
        String dest = line_read.split("\t")[1];
        boolean flag = true;
        for(Entry<Integer, Set<String>> entry:vertices_map.entrySet()) {
          if(entry.getValue().contains(source)) {
            flag = false;
          }
        }
        if(flag) {
          int bucket = divideIntoBucket();
          vertices_map.get(bucket).add(source);
        }
        
        flag = true;
        for(Entry<Integer, Set<String>> entry:vertices_map.entrySet()) {
          if(entry.getValue().contains(dest)) {
            flag = false;
          }
        }
        if(flag) {
          int bucket = divideIntoBucket();
          vertices_map.get(bucket).add(dest);
        }
      }
      reader.close();
      
      reader = new BufferedReader(new FileReader(file));
      line_read = null;
      while((line_read = reader.readLine()) != null) {
        String source = line_read.split("\t")[0];
        int bucket = 0;
        for(int i=1; i<=8; i++) {
          if(vertices_map.get(i).contains(source)) {
            bucket = i;
            break;
          }
        }
        edges_map.get(bucket).add(line_read);
      }
      reader.close();
      
      for(int i=1; i<=8; i++) {
        file = new File("input/" + i + "vertex.txt");
        if(!file.exists()) {
          file.createNewFile();
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        for(String s:vertices_map.get(i)) {
          bw.write(s + "\n");
        }
        bw.close();
        
        file = new File("input/" + i + "edge.txt");
        if(!file.exists()) {
          file.createNewFile();
        }
        fw = new FileWriter(file.getAbsoluteFile());
        bw = new BufferedWriter(fw);
        for(String s:edges_map.get(i)) {
          bw.write(s + "\n");
        }
        bw.close();
      }
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
  }
  
  /**
   * Output vertices and their user-defined values to a file.
   * 
   * @param filePath, the output file path.
   */
  public void result(String filePath) {
    File file = new File(filePath);
    try {
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      for(Worker<V, L> worker:workers) {
        Set<Vertex<V, L>> vertices = worker.getVertices();
        for(Vertex<V, L> v:vertices) {
          if(v.getValue() instanceof Integer && (Integer)v.getValue() == Integer.MAX_VALUE) {
            bw.write(v.getID() + "\tINF\n");
          } else if(v.getValue() instanceof Double && (Double)v.getValue() == Double.MAX_VALUE) {
            bw.write(v.getID() + "\tINF\n");
          } else {
            bw.write(v.getID() + "\t" + v.getValue() + "\n");
          }
        }
      }
      bw.close();
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Partition graph data into 8 buckets.
   * Generate a random number to determine which bucket to put in.
   * 
   * @return the bucket number which range from 1 to 8.
   */
  private int divideIntoBucket() {
    double random = Math.random();
    int bucket;
    if(random > 0.5) {
      if(random > 0.75) {
        if(random > 0.875) {
          bucket = 7;
        } else {
          bucket = 8;
        }
      } else {
        if(random > 0.625) {
          bucket = 6;
        } else {
          bucket = 5;
        }
      }
    } else {
      if(random > 0.25) {
        if(random > 0.375) {
          bucket = 4;
        } else {
          bucket = 3;
        }
      } else {
        if(random > 0.125) {
          bucket = 2;
        } else {
          bucket = 1;
        }
      }
    }
    return bucket;
  }
  
}
