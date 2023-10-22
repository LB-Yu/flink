/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.my.queries.minibatch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.time.Duration;

/**
 * An example showing how mini-batch can prevent data jitter.
 *
 * <p>How to run:
 *
 * <ul>
 *   <li>Open a terminal and run: nc-lk 9999
 *   <li>Run this example
 *   <li>Input the below lines in terminal:
 *       <pre>
 *       1,2023-12-19
 *       2,2023-12-19
 *       11,2023-12-19
 *     </pre>
 * </ul>
 */
public class MiniBatchUVExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        configuration.set(
                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(5));
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 2L);

        String source =
                "CREATE TABLE source (\n"
                        + "  user_id INT,\n"
                        + "  `day` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = 'localhost',\n"
                        + "  'port' = '9999',\n"
                        + "  'format' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sql =
                "SELECT `day`, SUM(cnt) total\n"
                        + "FROM (\n"
                        + "  SELECT `day`, MOD(user_id, 10), COUNT(DISTINCT user_id) as cnt\n"
                        + "  FROM source GROUP BY `day`, MOD(user_id, 10))\n"
                        + "GROUP BY `day`;";
        tEnv.sqlQuery(sql).execute().print();
    }
}
