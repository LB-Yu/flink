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

package org.apache.flink.table.examples.java.my.queries;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;

import org.apache.calcite.rel.rules.AggregateProjectMergeRule;

import java.io.IOException;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/**
 * Learn the following from GROUP query.
 *
 * <ul>
 *   <li>1. Process of optimizing:
 *       <ul>
 *         <li>Related rule of non mini batch: {@link AggregateProjectMergeRule}, {@link
 *             FlinkExpandConversionRule}
 *       </ul>
 *   <li>2. Process of retract:
 *       <ul>
 *         <li>Non mini batch: {@link GroupAggFunction}
 *       </ul>
 * </ul>
 */
public class GroupExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //        Configuration configuration = tEnv.getConfig().getConfiguration();
        //        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        //        configuration.set(
        //                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
        // Duration.ofSeconds(5));
        //        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5000L);

        // write source data into temporary file and get the absolute path
        String contents = "1,1\n" + "1,102\n" + "3,103\n" + "4,4\n" + "5,105\n" + "6,106";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE source (\n"
                        + "  a INT,\n"
                        + "  b INT,\n"
                        + "  proctime AS PROCTIME(),\n"
                        + "  primary key (a) NOT ENFORCED"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sourceView =
                "CREATE VIEW s1 AS\n"
                        + "SELECT a, b\n"
                        + "FROM (\n"
                        + "    SELECT a, b,\n"
                        + "        ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC) AS rownum\n"
                        + "\tFROM source\n"
                        + ") WHERE rownum <= 1;";
        tEnv.executeSql(sourceView);

        String sink =
                "CREATE TABLE sink (\n"
                        + "  a INT,\n"
                        + "  v BIGINT,\n"
                        + "  primary key (v) NOT ENFORCED"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";
        tEnv.executeSql(sink);

        //        String sql = "select count(*) from source";
        String insert = "insert into sink select a, count(*) from s1 group by a";
        tEnv.executeSql(insert);

        //        tEnv.sqlQuery(sql).execute().print();
    }
}
