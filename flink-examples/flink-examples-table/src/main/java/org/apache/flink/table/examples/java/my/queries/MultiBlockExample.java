package org.apache.flink.table.examples.java.my.queries;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/**
 * An example from {@link org.apache.flink.table.planner.plan.optimize.RelNodeBlock}.
 * */
public class MultiBlockExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,1,3,2019-12-12 00:00:01\n"
                        + "2,102,4,2019-12-12 00:00:02\n"
                        + "3,103,3,2019-12-12 00:00:04\n"
                        + "4,4,3,2019-12-12 00:00:06\n"
                        + "5,105,2,2019-12-12 00:00:05\n"
                        + "6,106,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE source (\n"
                        + "  a INT,\n"
                        + "  b INT,\n"
                        + "  c INT,\n"
                        + "  ts TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sink1 =
                "CREATE TABLE sink2 (\n"
                        + "  a1 INT,\n"
                        + "  b1 INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";
        tEnv.executeSql(sink1);

        String sink2 =
                "CREATE TABLE sink1 (\n"
                        + "  a2 INT,\n"
                        + "  b2 INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";
        tEnv.executeSql(sink2);

        Table p1 = tEnv.sqlQuery("select a, b, c from source");
        tEnv.createTemporaryView("p1", p1);

        Table f11 = tEnv.sqlQuery("select * from p1 where a > 0");
        tEnv.createTemporaryView("f11", f11);
        Table f12 = tEnv.sqlQuery("select * from p1 where c is not null");
        tEnv.createTemporaryView("f12", f12);

        Table p21 = tEnv.sqlQuery("select a as a1, b as b1 from f11");
        tEnv.createTemporaryView("p21", p21);
        Table p22 = tEnv.sqlQuery("select b as b2, c as c2 from f12");
        tEnv.createTemporaryView("p22", p22);

        Table join = tEnv.sqlQuery("select * from p21 join p22 on a1 = b2");
        tEnv.createTemporaryView("join", join);

        Table f21 = tEnv.sqlQuery("select * from `join` where a1 >= 70");
        tEnv.createTemporaryView("f21", f21);
        Table f22 = tEnv.sqlQuery("select * from `join` where a1 < 70");
        tEnv.createTemporaryView("f22", f22);

        Table p31 = tEnv.sqlQuery("select a1, b1 from f21");
        tEnv.createTemporaryView("p31", p31);
        Table p32 = tEnv.sqlQuery("select a1, c2 from f22");
        tEnv.createTemporaryView("p32", p32);

        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("insert into sink1 select * from p31");
        statementSet.addInsertSql("insert into sink2 select * from p32");
        statementSet.execute();
    }
}
