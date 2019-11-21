package statistics;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics. Used to count the number of vertices and edges stored per worker.
 * And count the calculation time and the number of communication of each worker in each superStep.
 * 
 */
public class Statistics {

  private Map<String, Integer> vertexNumber;
  private Map<String, Integer> edgeNumber;
  
  private Map<Integer, Map<String, Double>> computeTime;
  private Map<Integer, Map<String, Integer>> communicationNumber;
  
  public Statistics() {
    vertexNumber = new HashMap<String, Integer>();
    edgeNumber = new HashMap<String, Integer>();
    computeTime = new HashMap<Integer, Map<String, Double>>();
    communicationNumber = new HashMap<Integer, Map<String, Integer>>();
  }
  
  public void addVertexNumber(String worker_id, int number) {
    vertexNumber.put(worker_id, number);
  }
  
  public void addEdgeNumber(String worker_id, int number) {
    edgeNumber.put(worker_id, number);
  }
  
  public void addComputeTime(int superstep, String worker_id, double time) {
    Map<String, Double> tmp_map;
    if(computeTime.get(superstep) == null) {
      tmp_map = new HashMap<String, Double>();
    } else {
      tmp_map = computeTime.get(superstep); 
    }
    tmp_map.put(worker_id, time);
    computeTime.put(superstep, tmp_map);
  }
  
  public void addCommunicationNumber(int superstep, String worker_id, int number) {
    Map<String, Integer> tmp_map;
    if(communicationNumber.get(superstep) == null) {
      tmp_map = new HashMap<String, Integer>();
    } else {
      tmp_map = communicationNumber.get(superstep);
    }
    tmp_map.put(worker_id, number);
    communicationNumber.put(superstep, tmp_map);
  }
  
}
