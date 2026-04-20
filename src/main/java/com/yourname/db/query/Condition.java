package com.yourname.db.query;

public class Condition {

    //this class represents "WHERE" clause for SQL statements
    private String column;
    private String value;
    private String operator; // "=" "<" ">"

    public Condition(String column, String operator, String value) {
        this.column = column;
        this.value = value;
        this.operator = operator;
    }
}
