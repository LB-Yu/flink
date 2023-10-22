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

package org.apache.flink.streaming.examples.my;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SSGExample {

    public static void main(String[] args) throws Exception {
        SlotSharingGroup ssgA =
                SlotSharingGroup.newBuilder("a").setCpuCores(1.0).setTaskHeapMemoryMB(100).build();

        SlotSharingGroup ssgB =
                SlotSharingGroup.newBuilder("b").setCpuCores(0.5).setTaskHeapMemoryMB(100).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> input1 = env.fromElements(1, 3, 5, 7, 9).slotSharingGroup(ssgA);
        DataStream<Integer> input2 = env.fromElements(2, 4, 6, 8, 10).slotSharingGroup(ssgA);

        DataStream<Integer> union = input1.union(input2);
        union.print().slotSharingGroup(ssgB);

        System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());
        //        env.execute();
    }
}
