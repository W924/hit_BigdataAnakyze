package sssp;

import java.util.List;
import vertex.Vertex;

/**
 * The Vertex class of Single Source Shortest Path
 * which extends Vertex<V, L>, and rewrites the compute() function.
 *
 */
public class ShortestPathVertex extends Vertex<Integer, Integer> {

  String source;
  
  public ShortestPathVertex(String id, String source) {
    super(id);
    this.source = source;
  }
  
  /**
   * Defines an active vertex in a SuperStep what to do.
   * 
   * if the vertex is in SuperStep 0, initialize it;
   * 
   * if the vertex is not in SuperStep 0, receive messages, set the value of vertex to
   * the minimum value in messages and the original value.During this process, if the value
   * changes, send messages to adjacent vertices the value.
   * 
   */
  public void compute(Vertex<Integer, Integer> vertex, List<Integer> messages) {
    // superstep0 用于初始化
    // 源点value为0, state为active; 其他点value为INF, state为inactive
    if(vertex.getSuperStep() == 0) {
      if(vertex.getID().equals(source)) {
        vertex.voltToActive();
        vertex.mutableValue(0);
      } else {
        vertex.voltToHalt();
        vertex.mutableValue(Integer.MAX_VALUE);
      }
      
    } else {
    // 每轮超步中, active的顶点将接收到的最短距离与自身value比较
    // 如果value更新, 向邻接顶点发送消息, 然后状态改为inactive
    // 否则直接将状态改为inactive
      int mindist = vertex.getValue();
      for(int i=0; i<messages.size(); i++) {
        mindist = Math.min(mindist, messages.get(i));
      }
      if(mindist < Integer.valueOf(vertex.getValue()) || (vertex.getID().equals(source) && vertex.getSuperStep() == 1)) {
        vertex.mutableValue(mindist);
        for(String dest_vertex:vertex.getEdges()) {
          vertex.sendMessageTo(dest_vertex, mindist + 1);
        }
      }
      vertex.voltToHalt();
    }
  }
 
}
