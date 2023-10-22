package org.apache.flink.streaming.examples.my;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * This example is for observing the process of generating the {@link StreamGraph}.
 * */
public class StreamGraphExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> input1 = env.fromElements(1, 3, 5, 7, 9);
        DataStream<Integer> input2 = env.fromElements(2, 4, 6, 8, 10);

        DataStream<Integer> union = input1.union(input2);
        union.print();

//        System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());
        env.execute();
    }
}
