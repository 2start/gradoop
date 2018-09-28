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

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.VertexLabeledEdgeListDataSourceTest;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CSVDataSourceTest extends CSVTestBase {

  @Test
  public void testRead() throws Exception {
    String csvPath = CSVDataSourceTest.class
      .getResource("/data/csv/input_graph_collection")
      .getFile();

    String gdlPath = CSVDataSourceTest.class
      .getResource("/data/csv/expected/expected_graph_collection.gdl")
      .getFile();

    DataSource dataSource = new CSVDataSource(csvPath, getConfig());
    GraphCollection input = dataSource.getGraphCollection();
    GraphCollection expected = getLoaderFromFile(gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2");

    collectAndAssertTrue(input.equalsByGraphElementData(expected));
  }

  /**
   * Test reading a logical graph from csv files with properties
   * that are supported by csv source and sink
   *
   * @throws Exception on failure
   */
  @Test
  public void testReadExtendedProperties() throws Exception {
    LogicalGraph expected = getExtendedLogicalGraph();

    String csvPath = CSVDataSourceTest.class
      .getResource("/data/csv/input_extended_properties")
      .getFile();

    DataSource dataSource = new CSVDataSource(csvPath, getConfig());
    LogicalGraph sourceLogicalGraph = dataSource.getLogicalGraph();

    collectAndAssertTrue(sourceLogicalGraph.equalsByElementData(expected));

    dataSource.getLogicalGraph().getEdges().collect()
      .forEach(this::checkProperties);
    dataSource.getLogicalGraph().getVertices().collect()
      .forEach(this::checkProperties);
  }

  /**
   * Reads a logical graph from a csv graph collection and selects a specified graph head.
   */
  @Test
  public void testReadLogicalGraphWithSelectedGraphHead() {
    String csvPath = CSVDataSourceTest.class
      .getResource("/data/csv/input_graph_collection")
      .getFile();

    CSVDataSource dataSource = new CSVDataSource(csvPath, getConfig());
    LogicalGraph input = dataSource.getLogicalGraph("g1");

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0:g1 {a:\"graph1\",b:2.75d}[" +
        "(v_B_0:B {a:1234L,b:true,c:0.123d})" +
        "(v_B_1:B {a:5678L,b:false,c:4.123d})" +
        "(v_B_0)-[e_b_1:b{a:2718L}]->(v_B_1)]");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g1");

    // equals by data is used to compare the graph heads.
    expected.equalsByData(input);
  }
}
