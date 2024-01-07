/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** Utility class for converting sql to {@link Operation}s. */
public class OperationsConverter implements Iterator<Operation> {

    private final Iterator<SqlNode> sqlNodeIterator;
    private final FlinkPlannerImpl planner;
    private final CatalogManager catalogManager;

    private boolean isInStatementSet;
    private List<ModifyOperation> statementSetOperations;

    public OperationsConverter(
            SqlNodeList sqlNodeList, FlinkPlannerImpl planner, CatalogManager catalogManager) {
        this.sqlNodeIterator = sqlNodeList.iterator();
        this.planner = planner;
        this.catalogManager = catalogManager;
    }

    @Override
    public boolean hasNext() {
        return sqlNodeIterator.hasNext();
    }

    @Override
    public Operation next() {
        Optional<Operation> operation = Optional.empty();
        while (!operation.isPresent()) {
            operation = convert();
        }
        return operation.get();
    }

    private Optional<Operation> convert() {
        if (!sqlNodeIterator.hasNext() && isInStatementSet) {
            throw new ValidationException(
                    "'BEGIN STATEMENT SET;' must be used with 'END;', but no 'END;' founded.");
        }

        SqlNode sqlNode = sqlNodeIterator.next();
        Operation operation =
                SqlNodeToOperationConversion.convert(planner, catalogManager, sqlNode)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                "Unsupported statement: " + sqlNode.toString()));
        if (isInStatementSet
                && !(operation instanceof SinkModifyOperation
                        || operation instanceof StatementSetOperation
                        || operation instanceof EndStatementSetOperation)) {
            throw new ValidationException(
                    "Only 'INSERT INTO' statement is allowed in statement set.");
        }

        if (operation instanceof ModifyOperation) {
            if (isInStatementSet) {
                statementSetOperations.add((ModifyOperation) operation);
                return Optional.empty();
            } else {
                return Optional.of(operation);
            }
        } else if (operation instanceof BeginStatementSetOperation) {
            isInStatementSet = true;
            statementSetOperations = new ArrayList<>();
        } else if (operation instanceof EndStatementSetOperation) {
            if (!isInStatementSet) {
                throw new ValidationException(
                        "'END;' must be used after 'BEGIN STATEMENT SET;', "
                                + "but no 'BEGIN STATEMENT SET;' founded.");
            }
            if (statementSetOperations.isEmpty()) {
                throw new ValidationException("NO statements in statement set.");
            }

            isInStatementSet = false;
            StatementSetOperation statementSetOperation =
                    new StatementSetOperation(statementSetOperations);
            return Optional.of(statementSetOperation);
        } else {
            return Optional.of(operation);
        }
        return Optional.empty();
    }
}
