package com.yourname.db.query;



public class Parser {

    private ParsedStatement parse(String sql){
        String[] parsedString = sql.split("\\s+");

        for (int i = 0; i < parsedString.length; i++) {
            parsedString[i] = parsedString[i].replaceAll("[()\"',]", "");
        }

        ParsedStatement parsedStatement;

        switch (parsedString[0]) {
            case "SELECT":
                parsedStatement = parseSelect(parsedString);
                break;
            case "DELETE":
                parsedStatement = parseDelete(parsedString);
                break;
            case "INSERT":
                parsedStatement = parseInsert(parsedString);
                break;
            case "UPDATE":
                parsedStatement = parseSelect(parsedString);
                break;
        }

        return parsedStatement;
    }

    private ParsedStatement parseInsert(String[] parsedString) {
    }
    // DELETE FROM users WHERE id = 5
    private ParsedStatement parseDelete(String[] parsedString) {
        ParsedStatement ps = new ParsedStatement();
        ps.statementType = StatementType.DELETE;
        boolean where = false;
        int whereIndex = 0;
        for (int i = 0; i < parsedString.length; i++) {
            if (parsedString[i].equals("WHERE")) {
                whereIndex = i;
                where = true;
            }
        }
        if (where) {
            Condition condition = parseCondition(parsedString, whereIndex);
            ps.condition = condition;
        }
        ps.tableName = parsedString[2];
        return ps;
    }

    private ParsedStatement parseSelect(String[] parsedString) {
        ParsedStatement ps = new ParsedStatement();
        ps.statementType = StatementType.SELECT;
        boolean where = false;
        int whereIndex = 0;
        for (int i = 0; i < parsedString.length; i++) {
            if (parsedString[i].equals("WHERE")) {
                whereIndex = i;
                where = true;
            }
        }
        if (where) {
            Condition condition = parseCondition(parsedString, whereIndex);
            ps.condition = condition;
        }
        ps.tableName = parsedString[3];
        return ps;
    }
    private Condition parseCondition(String[] tokens, int whereIndex){
        Condition condition = new Condition(tokens[whereIndex + 1], tokens[whereIndex + 2] , tokens[whereIndex + 3]);
        return condition;
    }
}
