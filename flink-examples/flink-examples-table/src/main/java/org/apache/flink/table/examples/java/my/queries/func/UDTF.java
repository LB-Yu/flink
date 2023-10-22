package org.apache.flink.table.examples.java.my.queries.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<y INT, m INT, d INT>"))
public class UDTF extends TableFunction<Row> {

    @Override
    public void open(FunctionContext context) throws Exception {
        System.out.println("++open");
    }

    public void eval(String column) {
        System.out.println("++eval");
        String[] split = column.split("-");
        collect(
                Row.of(
                        Integer.parseInt(split[0]),
                        Integer.parseInt(split[1]),
                        Integer.parseInt(split[2])));
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }
}
