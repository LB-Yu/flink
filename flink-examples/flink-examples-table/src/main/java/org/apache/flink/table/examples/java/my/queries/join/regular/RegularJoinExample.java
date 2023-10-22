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

package org.apache.flink.table.examples.java.my.queries.join.regular;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegularJoinExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        String source1 =
                "CREATE TABLE s1 (\n"
                        + "  id INT,\n"
                        + "  key STRING\n"
                        + "  ,PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = 'localhost',\n"
                        + "  'port' = '10001',\n"
                        + "  'byte-delimiter' = '10',\n"
                        + "  'format' = 'changelog-csv',\n"
                        + "  'changelog-csv.column-delimiter' = '|'\n"
                        + ");";
        tEnv.executeSql(source1);

        String source2 =
                "CREATE TABLE s2 (\n"
                        + "  id INT,\n"
                        + "  key STRING\n"
                        + "  ,PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = 'localhost',\n"
                        + "  'port' = '10002',\n"
                        + "  'byte-delimiter' = '10',\n"
                        + "  'format' = 'changelog-csv',\n"
                        + "  'changelog-csv.column-delimiter' = '|'\n"
                        + ");";
        tEnv.executeSql(source2);

        String source3 =
                "CREATE TABLE s3 (\n"
                        + "  id INT,\n"
                        + "  key STRING\n"
                        + "  ,PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = 'localhost',\n"
                        + "  'port' = '10003',\n"
                        + "  'byte-delimiter' = '10',\n"
                        + "  'format' = 'changelog-csv',\n"
                        + "  'changelog-csv.column-delimiter' = '|'\n"
                        + ");";
        tEnv.executeSql(source3);

        //        String sql =
        //                "SELECT s1.*, s2.*\n"
        //                        + "FROM s1\n"
        //                        + "FULL JOIN s2 ON s1.id = s2.id";
        //        tEnv.sqlQuery(sql).execute().print();

        String sql =
                "SELECT s1.*, s2.*, s3.*\n"
                        + "FROM s1\n"
                        + "FULL JOIN s2 ON s1.id = s2.id\n"
                        + "FULL JOIN s3 ON s2.id = s3.id";
        tEnv.sqlQuery(sql).execute().print();
    }
}
