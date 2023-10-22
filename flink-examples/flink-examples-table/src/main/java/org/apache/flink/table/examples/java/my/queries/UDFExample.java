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
import org.apache.flink.table.examples.java.my.queries.func.UDAF;
import org.apache.flink.table.examples.java.my.queries.func.UDF;
import org.apache.flink.table.examples.java.my.queries.func.UDTAF;
import org.apache.flink.table.examples.java.my.queries.func.UDTF;

import java.io.IOException;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/** An example of UDF. */
public class UDFExample {

    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createFunction("UDF", UDF.class);
        tEnv.createFunction("UDTF", UDTF.class);
        tEnv.createFunction("UDAF", UDAF.class);
        tEnv.createFunction("UDTAF", UDTAF.class);

        String contents =
                "1,2023-10-01\n"
                        + "31,2023-10-02\n"
                        + "61,2023-10-03\n"
                        + "91,2023-10-04\n"
                        + "101,2023-10-05\n"
                        + "131,2023-10-06";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE source (\n"
                        + "  a INT,\n"
                        + "  b STRING\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        // UDF repeated call case
        tEnv.sqlQuery(
                        "SELECT\n"
                                + "\tsplit_index(r, '|', 0) AS y,\n"
                                + "\tsplit_index(r, '|', 1) AS m,\n"
                                + "\tsplit_index(r, '|', 2) AS d\n"
                                + "FROM (\n"
                                + "  SELECT UDF(b) AS r FROM source\n"
                                + ");")
                .execute()
                .print();
        //        tEnv.sqlQuery(
        //                "SELECT "
        //                        + "split_index(UDF(b), '|', 0) AS y, "
        //                        + "split_index(UDF(b), '|', 1) AS m, "
        //                        + "split_index(UDF(b), '|', 2) AS d "
        //                        + "FROM source WHERE UDF(b) <> 'NULL'").execute().print();
        //        tEnv.sqlQuery(
        //                "SELECT\n"
        //                        + "\tsplit_index(r, '|', 0) AS y,\n"
        //                        + "\tsplit_index(r, '|', 1) AS m,\n"
        //                        + "\tsplit_index(r, '|', 2) AS d\n"
        //                        + "FROM (\n"
        //                        + "  SELECT UDF(b) AS r FROM source\n"
        //                        + ")\n"
        //                        + "WHERE r <> 'NULL';").execute().print();

        // Scalar function
        //        tEnv.sqlQuery("SELECT UDF(b) FROM source;").execute().print();

        // Table function
        //        tEnv.sqlQuery(
        //                "SELECT y, m, d FROM source, LATERAL TABLE(UDTF(b)) as T(y, m, d);")
        //                .execute().print();

        // Aggregate function
        //        tEnv.sqlQuery("SELECT UDAF(a) FROM source GROUP BY a").execute().print();

        // Table aggregate function
        //        tEnv.from("source")
        //                .groupBy($("a"))
        //                .flatAggregate(call("UDTAF", $("a")).as("value", "rank"))
        //                .select($("value"), $("rank"))
        //                .execute().print();
    }
}
