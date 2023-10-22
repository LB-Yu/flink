package org.apache.flink.table.examples.java.my.queries;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

public class TumblingWindowExample {

    public static void main(String[] args) throws Exception {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,beer,3,2019-12-12 00:00:01\n"
                        + "1,diaper,4,2019-12-12 00:00:02\n"
                        + "2,pen,3,2019-12-12 00:00:04\n"
                        + "2,rubber,3,2019-12-12 00:00:06\n"
                        + "3,rubber,2,2019-12-12 00:00:05\n"
                        + "4,beer,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE orders (\n"
                        + "  user_id INT,\n"
                        + "  product STRING,\n"
                        + "  amount INT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl);

        // run a SQL query on the table and retrieve the result as a new Table
        String query =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*) order_num,\n"
                        + "  SUM(amount) total_amount,\n"
                        + "  COUNT(DISTINCT product) unique_products\n"
                        + "FROM TABLE (CUMULATE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' SECOND, INTERVAL '5' SECOND))\n"
                        + "GROUP BY window_start, window_end";

        System.out.println(tEnv.explainSql(query));

        tEnv.executeSql(query).print();
        // should output:
        // +----+--------------------------------+--------------+--------------+-----------------+
        // | op |                   window_start |    order_num | total_amount | unique_products |
        // +----+--------------------------------+--------------+--------------+-----------------+
        // | +I |        2019-12-12 00:00:00.000 |            3 |           10 |               3 |
        // | +I |        2019-12-12 00:00:05.000 |            3 |            6 |               2 |
        // +----+--------------------------------+--------------+--------------+-----------------+

    }
}
