/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nus.cool.core.iceberg.aggregator;

/** Factory class of OLAP aggregators. */
public class AggregatorFactory {

  /** OLAP Aggregator types. */
  public enum AggregatorType {
    COUNT,

    SUM,

    AVERAGE,

    MAX,

    MIN,

    DISTINCTCOUNT
  }

  /**
   * Create an OLAP aggregator of a type.
   *
   * @param operator type of aggregator
   * @return constructed aggregagtor
   */
  public Aggregator create(AggregatorType operator) {
    switch (operator) {
      case COUNT:
        return new CountAggregator();
      case SUM:
        return new SumAggregator();
      case AVERAGE:
        return new AverageAggregator();
      case MAX:
        return new MaxAggregator();
      case MIN:
        return new MinAggregator();
      case DISTINCTCOUNT:
        return new CountDistinctAggregator();
      default:
        throw new IllegalArgumentException("Unknown aggregator operator: " + operator);
    }
  }
}
