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
    // superstep0 ���ڳ�ʼ��
    // Դ��valueΪ0, stateΪactive; ������valueΪINF, stateΪinactive
    if(vertex.getSuperStep() == 0) {
      if(vertex.getID().equals(source)) {
        vertex.voltToActive();
        vertex.mutableValue(0);
      } else {
        vertex.voltToHalt();
        vertex.mutableValue(Integer.MAX_VALUE);
      }
      
    } else {
    // ÿ�ֳ�����, active�Ķ��㽫���յ�����̾���������value�Ƚ�
    // ���value����, ���ڽӶ��㷢����Ϣ, Ȼ��״̬��Ϊinactive
    // ����ֱ�ӽ�״̬��Ϊinactive
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
