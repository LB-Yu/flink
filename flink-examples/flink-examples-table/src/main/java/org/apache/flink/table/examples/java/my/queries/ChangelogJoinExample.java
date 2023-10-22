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

import java.io.IOException;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/** An example to show changelog events in Deduplicate and Group Aggregation. */
public class ChangelogJoinExample {

    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        String contents1 = "1,10\n" + "1,20\n";
        String path1 = createTempFile(contents1);

        String contents2 = "10,a1\n" + "20,b1\n";
        String path2 = createTempFile(contents2);

        String source1 =
                "CREATE TABLE source1 (\n"
                        + "  id BIGINT,\n"
                        + "  level BIGINT,\n"
                        + "  proctime AS PROCTIME(),\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path1
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source1);

        String s1View =
                "CREATE VIEW s1 AS\n"
                        + "SELECT id, level\n"
                        + "FROM (\n"
                        + "    SELECT id, level,\n"
                        + "        ROW_NUMBER() OVER (PARTITION BY id ORDER BY proctime DESC) AS rownum\n"
                        + "\tFROM source1\n"
                        + ") WHERE rownum <= 1;";
        tEnv.executeSql(s1View);

        String source2 =
                "CREATE TABLE s2 (\n"
                        + "  id BIGINT,\n"
                        + "  attr STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path2
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source2);

        String sql = "SELECT s1.*, s2.attr\n" + "FROM s1 JOIN s2 ON s1.level = s2.id;";

        tEnv.sqlQuery(sql).execute().print();
    }
}
