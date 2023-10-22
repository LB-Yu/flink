package org.apache.flink.table.examples.java.my.queries.func;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class UDF extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        System.out.println("++open");
    }

    public String eval(String column) {
        System.out.println("++eval");
        String[] split = column.split("-");
        return String.join("|", split);
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }
}
