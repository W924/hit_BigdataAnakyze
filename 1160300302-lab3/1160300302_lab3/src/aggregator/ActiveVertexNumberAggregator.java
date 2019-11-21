package aggregator;

import vertex.Vertex;

/**
 * The aggregator to statistic the number of vertex in the graph.
 * SuperStep 0 start this aggerator, then shut down automatically.
 *
 */
public class ActiveVertexNumberAggregator implements Aggregator {

  private double vertexNumber;
  private boolean autoClose;
  
  public ActiveVertexNumberAggregator() {
    vertexNumber = 0;
    this.autoClose = false;
  }
  
  public ActiveVertexNumberAggregator(boolean autoClose) {
    vertexNumber = 0;
    this.autoClose = autoClose;
  }
  
  @Override
  public Aggregator getInstance(boolean autoClose) {
    return new VertexNumberAggregator(autoClose);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public double report(Vertex vertex) {
    if(vertex.getFlag()) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public void aggregate(double value) {
    vertexNumber += value;
  }

  @Override
  public double getAggregateValue() {
    return vertexNumber;
  }

  @Override
  public boolean isAutoClosed() {
    return autoClose;
  }

  @Override
  public void initValue() {
    vertexNumber = 0;
  }

}
