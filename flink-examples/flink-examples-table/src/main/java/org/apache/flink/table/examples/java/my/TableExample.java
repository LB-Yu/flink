package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.examples.java.basics.StreamSQLExample;

import java.util.Arrays;

public class TableExample {

    public static void main(String[] args) {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<StreamSQLExample.Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new StreamSQLExample.Order(1L, "beer", 3),
                                new StreamSQLExample.Order(1L, "diaper", 4),
                                new StreamSQLExample.Order(3L, "rubber", 2)));

        final Table tableA = tableEnv.fromDataStream(orderA);

        tableA.execute().print();
    }
}
