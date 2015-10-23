/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.unary.sampling;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.keyselectors
  .EdgeSourceVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors
  .EdgeTargetVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.functions.mapfunctions.EdgeToGraphUpdater;
import org.gradoop.model.impl.functions.mapfunctions.VertexToGraphUpdater;
import org.gradoop.util.FlinkConstants;

import java.util.Random;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class RandomNodeSampling<VD extends VertexData, ED extends EdgeData,
  GD extends GraphData> implements
  UnaryGraphToGraphOperator<VD, ED, GD> {
  /**
   * relative amount of nodes in the result graph
   */
  private final Float sampleSize;

  /**
   * seed for the random number generator
   * if no seed is null, the random generator is created without seed
   */
  private final Long randomSeed;

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomNodeSampling(Float sampleSize) {
    this(sampleSize, null);
  }

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomNodeSampling(Float sampleSize, Long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> graph) throws
    Exception {

    final Long newGraphID = FlinkConstants.RANDOM_NODE_SAMPLING_GRAPH_ID;

    DataSet<Vertex<Long, VD>> vertices = graph.getVertices();
    vertices =
      vertices.filter(new VertexRandomFilter<VD>(sampleSize, randomSeed))
        .map(new VertexToGraphUpdater<VD>(newGraphID));

    JoinFunction<Edge<Long, ED>, Vertex<Long, VD>, Edge<Long, ED>> joinFunc =
      new JoinFunction<Edge<Long, ED>, Vertex<Long, VD>, Edge<Long, ED>>() {
        @Override
        public Edge<Long, ED> join(Edge<Long, ED> leftTuple,
          Vertex<Long, VD> rightTuple) throws Exception {
          return leftTuple;
        }
      };

    DataSet<Edge<Long, ED>> edges = graph.getEdges().join(vertices)
      .where(new EdgeSourceVertexKeySelector<ED>())
      .equalTo(new VertexKeySelector<VD>()).with(joinFunc).join(vertices)
      .where(new EdgeTargetVertexKeySelector<ED>())
      .equalTo(new VertexKeySelector<VD>()).with(joinFunc)
      .map(new EdgeToGraphUpdater<ED>(newGraphID));

    return LogicalGraph.fromDataSets(vertices, edges,
      graph.getGraphDataFactory().createGraphData(newGraphID),
      graph.getVertexDataFactory(), graph.getEdgeDataFactory(),
      graph.getGraphDataFactory());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomNodeSampling.class.getName();
  }

  /**
   * Creates a random value for each vertex and filters those that are below
   * a given threshold.
   *
   * @param <VD> vertex data type
   */
  private static class VertexRandomFilter<VD extends VertexData> implements
    FilterFunction<Vertex<Long, VD>> {
    /**
     * Threshold to decide if a vertex needs to be filtered.
     */
    private final Float threshold;
    /**
     * Random instance
     */
    private final Random randomGenerator;

    /**
     * Creates a new filter instance.
     *
     * @param sampleSize relative sample size
     * @param randomSeed random seed (can be {@code} null)
     */
    public VertexRandomFilter(Float sampleSize, Long randomSeed) {
      threshold = sampleSize;
      randomGenerator =
        (randomSeed != null) ? new Random(randomSeed) : new Random();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(Vertex<Long, VD> vertex) throws Exception {
      return randomGenerator.nextFloat() < threshold;
    }
  }
}
