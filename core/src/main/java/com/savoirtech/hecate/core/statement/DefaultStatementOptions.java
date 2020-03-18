package com.savoirtech.hecate.core.statement;

import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.List;
import java.util.function.Function;

public class DefaultStatementOptions implements StatementOptions {
    private final List<Function<Statement, Statement>> options;

    public DefaultStatementOptions(List<Function<Statement, Statement>> options) {
        this.options = options;
    }

    @Override
    public Statement applyTo(Statement statement) {
        Statement result = statement;
        for (Function<Statement, Statement> option : options) {
            result = option.apply(statement);
        }
        return result;
    }
}
