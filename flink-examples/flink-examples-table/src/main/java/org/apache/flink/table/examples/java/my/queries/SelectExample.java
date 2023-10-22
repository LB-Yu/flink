package org.apache.flink.table.examples.java.my.queries;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/**
 * An example of {@code SELECT & WHERE}.
 * */
public class SelectExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,1\n"
                        + "2,102\n"
                        + "3,103\n"
                        + "4,4\n"
                        + "5,105\n"
                        + "6,106";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE source (\n"
                        + "  a INT,\n"
                        + "  b INT\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);
        
        String sql = "select a + 100 from source where a < 10";

        System.out.println(tEnv.explainSql(sql));
        
        tEnv.sqlQuery(sql).execute().print();
    }
}
