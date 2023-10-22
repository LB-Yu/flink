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

/** An example of Deduplication. */
public class DeduplicationExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,Jack,clothing,101\n"
                        + "2,Mike,clothing,99\n"
                        + "2,Mike,clothing,99\n"
                        + "3,Bob,food,103";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE Orders (\n"
                        + "  order_id  STRING,\n"
                        + "  `user`      STRING,\n"
                        + "  product     STRING,\n"
                        + "  num         BIGINT,\n"
                        + "  proctime AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sql =
                "SELECT order_id, `user`, product, num\n"
                        + "FROM (\n"
                        + "  SELECT *,\n"
                        + "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) AS row_num\n"
                        + "  FROM Orders)\n"
                        + "WHERE row_num = 1";

        //        System.out.println(tEnv.explainSql(sql));

        tEnv.sqlQuery(sql).execute().print();
    }
}
