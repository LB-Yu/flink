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
public class ChangelogGroupExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // write source data into temporary file and get the absolute path
        String contents =
                "001,ZhongTong\n" + "002,YuanTong\n" + "003,ZhongTong\n" + "001,YuanTong\n";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE source (\n"
                        + "  order_id STRING,\n"
                        + "  tms_company STRING,\n"
                        + "  proctime AS PROCTIME()"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sql =
                "SELECT tms_company, count(DISTINCT order_id) AS order_cnt\n"
                        + "FROM (\n"
                        + "  SELECT order_id, tms_company\n"
                        + "  FROM (\n"
                        + "    SELECT order_id, tms_company,\n"
                        + "      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime DESC) AS rownum\n"
                        + "    FROM source\n"
                        + "  ) WHERE rownum = 1\n"
                        + ") GROUP BY tms_company;";

        tEnv.sqlQuery(sql).execute().print();
    }
}
