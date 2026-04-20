package com.yourname.db.query;

import java.util.List;

public class ParsedStatement {
    public Condition condition;
    public StatementType statementType;
    public String tableName;
    public List<String> values;
}
