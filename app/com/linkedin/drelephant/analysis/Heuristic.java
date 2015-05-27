package com.linkedin.drelephant.analysis;

/**
 * This interface defines the Heuristic rule interface.
 *
 * @param <T> An implementation that extends from HadoopApplicationData
 */
public interface Heuristic<T extends HadoopApplicationData> {
  /**
   * Given an application data instance, returns the analyzed heuristic result.
   *
   * @param data The data to analyze
   * @return The heuristic result
   */
  public HeuristicResult apply(T data);

  /**
   * Get the heuristic name
   *
   * @return the name
   */
  public String getHeuristicName();
}
