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

/** An example of lookup join. */
public class LookupJoinExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String contents =
                "1,1001,1.2,CNY,2019-12-12 00:00:01\n"
                        + "31,1002,3.56,CNY,2019-12-12 00:00:02\n"
                        + "61,1003,6.78,CNY,2019-12-12 00:00:04\n"
                        + "91,1004,9.87,CNY,2019-12-12 00:00:06\n"
                        + "101,1005,4.32,CNY,2019-12-12 00:00:05\n"
                        + "131,1006,6.99,CNY,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        String orders =
                "CREATE TABLE Orders (\n"
                        + "  order_id STRING,\n"
                        + "  customer_id INT,\n"
                        + "  price DECIMAL(32,2),\n"
                        + "  currency STRING,\n"
                        + "  order_time TIMESTAMP(3),\n"
                        + "  proc_time AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(orders);

        String customs =
                "CREATE TEMPORARY TABLE Customers (\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  country STRING,\n"
                        + "  zip STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'jdbc',\n"
                        + "  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',\n"
                        + "  'table-name' = 'customers'\n"
                        + ");";
        tEnv.executeSql(customs);

        String query =
                "SELECT o.order_id, o.price, c.country, c.zip\n"
                        + "FROM Orders AS o\n"
                        + "  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c\n"
                        + "    ON o.customer_id = c.id;";

        tEnv.sqlQuery(query).execute().print();
    }
}
