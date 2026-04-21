package com.yourname.db.query;

public class Condition {

    //this class represents "WHERE" clause for SQL statements
    public String column;
    public String value;
    public String operator; // "=" "<" ">"

    public Condition(String column, String operator, String value) {
        this.column = column;
        this.value = value;
        this.operator = operator;
    }
}
