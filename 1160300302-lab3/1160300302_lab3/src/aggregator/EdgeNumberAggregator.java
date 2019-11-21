package aggregator;

import vertex.Vertex;

/**
 * The aggregator to statistic the number of edge in the graph.
 * SuperStep 0 start this aggerator, then shut down automatically.
 *
 */
public class EdgeNumberAggregator implements Aggregator {

  private double edgeNumber;
  private boolean autoClose;
  
  public EdgeNumberAggregator() {
    edgeNumber = 0;
    this.autoClose = true;
  }
  
  public EdgeNumberAggregator(boolean autoClose) {
    edgeNumber = 0;
    this.autoClose = autoClose;
  }
  
  @Override
  public Aggregator getInstance(boolean autoClose) {
    return new EdgeNumberAggregator(autoClose);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public double report(Vertex vertex) {
    return vertex.getEdges().size();
  }

  @Override
  public void aggregate(double value) {
    edgeNumber += value;
  }

  @Override
  public double getAggregateValue() {
    return edgeNumber;
  }

  @Override
  public boolean isAutoClosed() {
    return autoClose;
  }

  @Override
  public void initValue() {
    edgeNumber = 0;
  }

}
