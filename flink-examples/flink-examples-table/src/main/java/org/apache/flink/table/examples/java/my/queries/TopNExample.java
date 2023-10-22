package org.apache.flink.table.examples.java.my.queries;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/**
 * An example of TopN.
 * */
public class TopNExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,clothing,jeans,101\n"
                        + "2,clothing,t-shirt,99\n"
                        + "3,food,milk,103";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE ShopSales (\n"
                        + "  product_id   STRING,\n"
                        + "  category     STRING,\n"
                        + "  product_name STRING,\n"
                        + "  sales        BIGINT"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sql =
                "SELECT *\n"
                        + "FROM (\n"
                        + "  SELECT *,\n"
                        + "    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num\n"
                        + "  FROM ShopSales)\n"
                        + "WHERE row_num >= 1 and row_num <= 2";

        System.out.println(tEnv.explainSql(sql));

        tEnv.sqlQuery(sql).execute().print();
    }
}
