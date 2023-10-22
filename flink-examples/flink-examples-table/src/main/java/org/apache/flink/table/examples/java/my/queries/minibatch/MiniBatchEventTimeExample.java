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
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.io.IOException;
import java.time.Duration;

/**
 * A Mini-batch calculation example to demonstrate the existence of multiple Window Aggregates.
 *
 * <p>How to run:
 *
 * <ul>
 *   <li>Open a terminal and run: nc-lk 9999
 *   <li>Run this example
 *   <li>Input the below lines in terminal:
 *       <pre>
 *         1,Jack,clothing,101,2023-12-23 10:00:00
 *         1,Jack,clothing,101,2023-12-23 10:00:03
 *         2,Mike,clothing,99,2023-12-23 10:00:06
 *         2,Mike,clothing,99,2023-12-23 10:00:11
 *         3,Bob,food,103,2023-12-23 10:00:16
 *     </pre>
 * </ul>
 */
public class MiniBatchEventTimeExample {

    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        configuration.set(
                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(15));
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5000L);

        String source =
                "CREATE TABLE Orders (\n"
                        + "  order_id STRING,\n"
                        + "  `user` STRING,\n"
                        + "  product STRING,\n"
                        + "  num BIGINT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts\n"
                        + ") WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = 'localhost',\n"
                        + "  'port' = '9999',\n"
                        + "  'format' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String orderView =
                "CREATE VIEW o_v AS\n"
                        + "SELECT order_id, `user`, product, num, ts\n"
                        + "FROM (\n"
                        + "  SELECT *,\n"
                        + "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY PROCTIME() ASC) AS row_num\n"
                        + "  FROM Orders)\n"
                        + "WHERE row_num = 1;";
        tEnv.executeSql(orderView);

        String sink1 =
                "CREATE TABLE sink1 (\n"
                        + "  window_start TIMESTAMP(3),\n"
                        + "  window_end TIMESTAMP(3),\n"
                        + "  total_num BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";
        tEnv.executeSql(sink1);

        String sink2 =
                "CREATE TABLE sink2 (\n"
                        + "  window_start TIMESTAMP(3),\n"
                        + "  window_end TIMESTAMP(3),\n"
                        + "  total_num BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";
        tEnv.executeSql(sink2);

        String window1 =
                "INSERT INTO sink1\n"
                        + "SELECT window_start, window_end, SUM(num) as total_num\n"
                        + "FROM TABLE(\n"
                        + "    TUMBLE(TABLE o_v, DESCRIPTOR(ts), INTERVAL '5' SECONDS))\n"
                        + "GROUP BY window_start, window_end;";

        String window2 =
                "INSERT INTO sink2\n"
                        + "SELECT window_start, window_end, SUM(num) as total_num\n"
                        + "FROM TABLE(\n"
                        + "    TUMBLE(TABLE o_v, DESCRIPTOR(ts), INTERVAL '10' SECONDS))\n"
                        + "GROUP BY window_start, window_end;";

        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(window1);
        statementSet.addInsertSql(window2);

        statementSet.execute();
    }
}
