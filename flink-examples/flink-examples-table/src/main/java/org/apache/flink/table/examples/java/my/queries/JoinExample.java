package org.apache.flink.table.examples.java.my.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public final class JoinExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Order> order =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "rubber", 2)));

        final DataStream<User> user =
                env.fromCollection(
                        Arrays.asList(
                                new User(1L, "Jack"),
                                new User(2L, "John"),
                                new User(3L, "Mary")));

        final Table tableOrder = tableEnv.fromDataStream(order);
        tableEnv.createTemporaryView("order", tableOrder);
        final Table tableUser = tableEnv.fromDataStream(user);
        tableEnv.createTemporaryView("user", tableUser);

        // join the two tables
        final Table result =
                tableEnv.sqlQuery(
                        "SELECT * FROM "
                                + "`order` join `user` "
                                + "on `order`.`user` = `user`.userId");

        tableEnv.toDataStream(result, Row.class).print();

        env.execute();
    }

    public static class Order {
        public Long user;
        public String product;
        public int amount;

        // for POJO detection in DataStream API
        public Order() {}

        // for structured type detection in Table API
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }

    public static class User {
        public long userId;
        public String name;

        public User() { }

        public User(long userId, String name) {
            this.userId = userId;
            this.name = name;
        }

        @Override
        public String toString() {
            return "User{" +
                    "userId=" + userId +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
