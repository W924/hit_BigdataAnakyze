package aggregator;

import vertex.Vertex;

/**
 * The aggregator to statistic the number of vertex in the graph.
 * SuperStep 0 start this aggerator, then shut down automatically.
 *
 */
public class VertexNumberAggregator implements Aggregator {

  private double vertexNumber;
  private boolean autoClose;
  
  public VertexNumberAggregator() {
    vertexNumber = 0;
    this.autoClose = true;
  }
  
  public VertexNumberAggregator(boolean autoClose) {
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
    return 1;
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
