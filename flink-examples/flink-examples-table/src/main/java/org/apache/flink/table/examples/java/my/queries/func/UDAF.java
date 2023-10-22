package org.apache.flink.table.examples.java.my.queries.func;

import org.apache.flink.table.functions.AggregateFunction;

public class UDAF extends AggregateFunction<Long, UDAF.Acc> {

    @Override
    public Long getValue(Acc acc) {
        return acc.sum;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    public void accumulate(Acc acc, Long value) {
        acc.sum += value;
    }

    public static class Acc {
        public long sum = 0L;
    }
}
