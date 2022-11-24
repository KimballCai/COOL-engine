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

import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.CohortQueryLayout;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Cohort selection operation.
 */
public class CohortSelection {
  /**
   * perform the cohort query to select users into a cohort.
   *
   * @param cubeRepo cubeRepo path: the path to all datasets, e.g., CubeRepo
   * @param queryPath query path: the path to the cohort query, e.g.,
   *                  datasets/health_raw/sample_query_selection/query.json
   */
  public static String performCohortSelection(String cubeRepo, String queryPath) throws IOException {
    CohortQueryLayout layout = CohortQueryLayout.readFromJson(queryPath);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);

    // start a new cool model and reload the cube
    CoolModel coolModel = new CoolModel(cubeRepo);
    coolModel.reload(cohortProcessor.getDataSource());
    CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());

    // get current dir path
    File currentVersion = coolModel.loadLatestVersion(cohortProcessor.getDataSource());
    CohortRet ret = cohortProcessor.process(cube);
    String cohortStoragePath = cohortProcessor.persistCohort(currentVersion.toString());
    coolModel.close();

    return cohortStoragePath;
  }


  public static void main(String[] args) throws IOException {
    String cubeRepo = args[0];
    String queryPath = args[1];

    try {
      String cohortStoragePath = performCohortSelection(cubeRepo, queryPath);

      System.out.println("[*] Cohort results are stored into " + cohortStoragePath);
    } catch (IOException e) {
      System.out.println(e);
    }
  }
}
