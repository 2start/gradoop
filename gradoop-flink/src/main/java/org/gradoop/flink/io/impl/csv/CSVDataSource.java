/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToGraphHead;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToVertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

import javax.xml.crypto.Data;

/**
 * A graph data source for CSV files.
 *
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 *
 * csvRoot
 *   |- vertices.csv # all vertex data
 *   |- edges.csv    # all edge data
 *   |- metadata.csv # Meta data for all data contained in the graph
 */
public class CSVDataSource extends CSVBase implements DataSource {

  /**
   * Creates a new CSV data source.
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config Gradoop Flink configuration
   */
  public CSVDataSource(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  /**
   * {@inheritDoc}
   *
   * graph heads will be disposed.
   */
  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  /**
   * Allows to read a graph head when importing a logical graph by passing the graph heads label.
   * @param graphHeadLabel the graph head from graphs.csv
   * @return logical graph with the selected graph head
   */
  public LogicalGraph getLogicalGraph(String graphHeadLabel) {
    GraphCollection graphCollection = getGraphCollection();

    DataSet<GraphHead> graphHeads = graphCollection.getGraphHeads();
    DataSet<GraphHead> selected = graphHeads.filter(g -> g.getLabel().equals(graphHeadLabel));

    DataSet<Vertex> vertices = graphCollection.getVertices();
    DataSet<Edge> edges = graphCollection.getEdges();

    LogicalGraphFactory factory = graphCollection.getConfig().getLogicalGraphFactory();

    return factory.fromDataSets(selected, vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection getGraphCollection() {
    DataSet<Tuple3<String, String, String>> metaData =
      MetaData.fromFile(getMetaDataPath(), getConfig());

    DataSet<GraphHead> graphHeads = getConfig().getExecutionEnvironment()
      .readTextFile(getGraphHeadCSVPath())
      .map(new CSVLineToGraphHead(getConfig().getGraphHeadFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Vertex> vertices = getConfig().getExecutionEnvironment()
      .readTextFile(getVertexCSVPath())
      .map(new CSVLineToVertex(getConfig().getVertexFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Edge> edges = getConfig().getExecutionEnvironment()
      .readTextFile(getEdgeCSVPath())
      .map(new CSVLineToEdge(getConfig().getEdgeFactory()))
      .withBroadcastSet(metaData, BC_METADATA);


    return getConfig().getGraphCollectionFactory().fromDataSets(graphHeads, vertices, edges);
  }
}
