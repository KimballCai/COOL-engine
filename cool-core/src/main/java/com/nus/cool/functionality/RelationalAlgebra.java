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

package com.nus.cool.functionality;

import java.util.List;

import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.model.CoolModel;

/** Relational algebra operation. */
public class RelationalAlgebra {

  /** Execute relational query. */
  public static void main(String[] args) throws Exception {
    // the path of dz file eg "COOL/cube"
    String datasetPath = args[0]; // e.g., "../datasetSource";=
    String cubeName = args[1]; // e.g., "tpc-h-10g"
    String operation = args[2]; // e.g., "select, O_ORDERPRIORITY, 2-HIGH"

    // load .dz file
    CoolModel coolModel = new CoolModel(datasetPath);
    coolModel.reload(cubeName);

    IcebergQuery query = coolModel.olapEngine.generateQuery(operation, cubeName);
    if (query == null) {
      coolModel.close();
      return;
    }

    // execute query
    List<BaseResult> result =
        coolModel.olapEngine.performOlapQuery(coolModel.getCube(cubeName), query);
    System.out.println(result.toString());
    coolModel.close();
  }
}
