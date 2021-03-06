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
package org.gradoop.storage.impl.accumulo.handler;

import org.apache.accumulo.core.data.Mutation;
import org.gradoop.common.model.api.entities.EPGMElement;

/**
 * accumulo row handler
 *
 * @param <R> row to read
 * @param <W> row to store
 */
public interface AccumuloRowHandler<R extends EPGMElement, W extends EPGMElement> {

  /**
   * write element to store
   *
   * @param mutation new row mutation
   * @param record EPGMElement to be write
   * @return row mutation after writer
   */
  Mutation writeRow(
    Mutation mutation,
    W record
  );

  /**
   * read row from origin epgm definition
   *
   * @param origin origin epgm definition
   * @return epgm row
   */
  R readRow(W origin);

}
