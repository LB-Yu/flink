package org.apache.flink.table.examples.java.my.queries;

import org.apache.calcite.rel.rules.AggregateProjectMergeRule;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.table.examples.java.my.Utils.createTempFile;

/**
 * Learn the following from GROUP query.
 *
 * <ul>
 *     <li>
 *         1. Process of optimizing:
 *         <ul>
 *             <li>Related rule of non mini batch: {@link AggregateProjectMergeRule}, {@link FlinkExpandConversionRule}</li>
 *         </ul>
 *     </li>
 *     <li>
 *         2. Process of retract:
 *         <ul>
 *             <li>Non mini batch: {@link GroupAggFunction}</li>
 *         </ul>
 *     </li>
 * </ul>
 * */
public class GroupExample {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        Configuration configuration = tEnv.getConfig().getConfiguration();
//        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
//        configuration.set(
//                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(5));
//        configuration.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5000L);

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

        String sql = "select count(*) from source";

//        System.out.println(tEnv.explainSql(sql));

        tEnv.sqlQuery(sql).execute().print();
    }
}
