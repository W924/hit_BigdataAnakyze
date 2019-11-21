package aggregator;

import vertex.Vertex;

/**
 * A simple aggregator, which only reduces input values from a single superStep.
 * 
 *
 * @param <V>
 */
public interface Aggregator {
  
  /**
   * Return a instance of Aggregator.
   * 
   * @param autoClose, automatically close. If the Aggregator just acts in superStep0, 
   *    this param is true; else, this param is false.
   * @return an Aggregator instance.
   */
  public Aggregator getInstance(boolean autoClose);
  
  /**
   * Define what is sent by each vertex.
   * 
   * @return the content sent by vertex.
   */
  @SuppressWarnings("rawtypes") 
  public double report(Vertex vertex);
  
  /**
   * Specify how the aggregate value is calculated.
   * 
   */
  public void aggregate(double value);
  
  /**
   * Get the aggregate value.
   * 
   * @return the aggregate value.
   */
  public double getAggregateValue();
  
  /**
   * Get the state of Aggregator if it's closed automatically.
   *
   */
  public boolean isAutoClosed();
  
  /**
   * Initial the aggregate value.
   * 
   */
  public void initValue();
  
}
