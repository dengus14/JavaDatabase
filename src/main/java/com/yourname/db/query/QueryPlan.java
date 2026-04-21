package com.yourname.db.query;

import java.util.List;

public class QueryPlan {

    public enum PlanType {
        SEQ_SCAN,
        INSERT,
        DELETE
    }

    public PlanType planType;
    public String tableName;
    public Condition condition;
    public List<String> values;

    //design pattern - factory method
    public static QueryPlan fromStatement(ParsedStatement ps) {
        QueryPlan plan = new QueryPlan();
        switch (ps.statementType){
            default:
                throw new IllegalArgumentException("Unknown statement type: " + ps.statementType);
            case INSERT:
                plan.planType = PlanType.INSERT;
                break;
            case DELETE:
                plan.planType = PlanType.DELETE;
                break;
            case SELECT:
                plan.planType = PlanType.SEQ_SCAN;
                break;

        }
        plan.tableName = ps.tableName;
        plan.condition = ps.condition;
        plan.values = ps.values;
        return plan;
    }
}
