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
    // superstep0���ڳ�ʼ��
    // ���ж���value��ʼ��Ϊ1(�������Զ��������ֵ), state��ʼ��Ϊactive
    if(vertex.getSuperStep() == 0) {
      vertex.mutableValue(1.0);
      vertex.voltToActive();
      
    }
    
    // ÿ�ֳ�����, ���㽫���յ�����Ϣ�е�ֵ���, �õ�sum
    // Ȼ���������valueΪ: 0.15 + 0.85 * sum
    if(vertex.getSuperStep() >= 1) {
      double sum = 0;
      for(int i=0; i<messages.size(); i++) {
        sum += messages.get(i);
      }
      vertex.mutableValue(0.15 + 0.85 * sum);
    }
    
    // �ڹ涨����������, ÿ���������ڽӶ��㷢������ǰ��valueֵ
    if(vertex.getSuperStep() <= iteration) {
      int n = vertex.getEdges().size();
      for(String dest_vertex:vertex.getEdges()) {
        vertex.sendMessageTo(dest_vertex, vertex.getValue() / n);
      }
    } else {
    // ���������涨������, ���ٷ�����Ϣ�Ҷ���״̬��Ϊinactive
      vertex.voltToHalt();
    }
  }
  
}
