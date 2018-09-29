/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with Hadoop framework for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class Driver {
    public static void main(String[] args) throws Exception {
        /*
         * Arg 0: HDFS path for transition data
         * Arg 1: HDFS path for initial page rank data
         * Arg 2: HDFS path for UnitMultiplication Reducer output
         * Arg 3: Times of convergence
         *
         * Note: HBase can be used as another approach.
         *
         * - Elton Hu (Sept. 28, 2018)
         */

        UnitMultiplication unitMultiplication = new UnitMultiplication();
        UnitSum unitSum = new UnitSum();

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String unitMultiplicationOutput = args[2];
        int count = Integer.parseInt(args[3]);

        for (int i = 0; i < count; ++i) {
            String[] unitMultiplicationArgs = {transitionMatrix, prMatrix + i, unitMultiplicationOutput + i};
            unitMultiplication.main(unitMultiplicationArgs);

            String[] unitSumArgs = {unitMultiplicationOutput + i, prMatrix + (i + 1)};
            unitSum.main(unitSumArgs);
        }
    }

}
