package pageRank;

import java.util.List;
import vertex.Vertex;

/**
 * The Vertex class of PageRank which extends Vertex<V, L>, and rewrites the compute() function.
 *
 */
public class PageRankVertex extends Vertex<Double, Double> {
  
  int iteration;
  
  public PageRankVertex(String id, int iteration) {
    super(id);
    this.iteration = iteration;
  }
  
  /**
   * Defines an active vertex in a SuperStep what to do.
   * 
   * if the vertex is in SuperStep 0, initialize its value to 1(customized), state to active;
   * 
   * if the vertex is not in SuperStep 0, compute the sum of messages, and update the value.
   * Within the specified number of iterations, send the value of the vertex to Adjacent vertices,
   * else send nothing, and change the state to inactive.
   * 
   */
  public void compute(Vertex<Double, Double> vertex, List<Double> messages) {
    // superstep0用于初始化
    // 所有顶点value初始化为1(可以是自定义的其他值), state初始化为active
    if(vertex.getSuperStep() == 0) {
      vertex.mutableValue(1.0);
      vertex.voltToActive();
      
    }
    
    // 每轮超步中, 顶点将接收到的消息中的值相加, 得到sum
    // 然后更新自身value为: 0.15 + 0.85 * sum
    if(vertex.getSuperStep() >= 1) {
      double sum = 0;
      for(int i=0; i<messages.size(); i++) {
        sum += messages.get(i);
      }
      vertex.mutableValue(0.15 + 0.85 * sum);
    }
    
    // 在规定超步轮数内, 每个顶点向邻接顶点发送自身当前的value值
    if(vertex.getSuperStep() <= iteration) {
      int n = vertex.getEdges().size();
      for(String dest_vertex:vertex.getEdges()) {
        vertex.sendMessageTo(dest_vertex, vertex.getValue() / n);
      }
    } else {
    // 迭代超过规定轮数后, 不再发送消息且顶点状态改为inactive
      vertex.voltToHalt();
    }
  }
  
}
