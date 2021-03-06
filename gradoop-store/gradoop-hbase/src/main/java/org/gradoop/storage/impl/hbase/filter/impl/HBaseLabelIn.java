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
package org.gradoop.storage.impl.hbase.filter.impl;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.impl.hbase.filter.api.HBaseElementFilter;
import org.gradoop.storage.common.predicate.filter.impl.LabelIn;

import javax.annotation.Nonnull;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_META;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.COL_LABEL;


/**
 * HBase label equality predicate implementation
 *
 * @param <T> EPGM element type
 */
public class HBaseLabelIn<T extends EPGMElement> extends LabelIn<HBaseElementFilter<T>>
  implements HBaseElementFilter<T> {

  /**
   * Create a new label equality filter
   *
   * @param labels label
   */
  public HBaseLabelIn(String... labels) {
    super(labels);
  }

  @Nonnull
  @Override
  public Filter toHBaseFilter() {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    for (String label : getLabels()) {
      SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
        Bytes.toBytesBinary(CF_META),
        Bytes.toBytesBinary(COL_LABEL),
        CompareFilter.CompareOp.EQUAL,
        Bytes.toBytesBinary(label)
      );
      filterList.addFilter(valueFilter);
    }
    return filterList;
  }
}
