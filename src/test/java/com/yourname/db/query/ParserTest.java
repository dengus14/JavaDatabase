package com.yourname.db.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParserTest {

    private Parser parser;

    @BeforeEach
    void setUp() {
        parser = new Parser();
    }

    @Test
    void parseInsertStatement() {
        ParsedStatement ps = parser.parse("INSERT INTO users VALUES (1, 'Alice', true)");

        assertEquals(StatementType.INSERT, ps.statementType);
        assertEquals("users", ps.tableName);
        assertEquals(3, ps.values.size());
        assertEquals("1", ps.values.get(0));
        assertEquals("Alice", ps.values.get(1));
        assertEquals("true", ps.values.get(2));
        assertNull(ps.condition);
    }

    @Test
    void parseSelectAllStatement() {
        ParsedStatement ps = parser.parse("SELECT * FROM users");

        assertEquals(StatementType.SELECT, ps.statementType);
        assertEquals("users", ps.tableName);
        assertNull(ps.condition);
    }

    @Test
    void parseSelectWithWhereClause() {
        ParsedStatement ps = parser.parse("SELECT * FROM users WHERE id = 2");

        assertEquals(StatementType.SELECT, ps.statementType);
        assertEquals("users", ps.tableName);
        assertNotNull(ps.condition);
        assertEquals("id", ps.condition.column);
        assertEquals("=", ps.condition.operator);
        assertEquals("2", ps.condition.value);
    }

    @Test
    void parseDeleteWithWhereClause() {
        ParsedStatement ps = parser.parse("DELETE FROM users WHERE id = 1");

        assertEquals(StatementType.DELETE, ps.statementType);
        assertEquals("users", ps.tableName);
        assertNotNull(ps.condition);
        assertEquals("id", ps.condition.column);
        assertEquals("=", ps.condition.operator);
        assertEquals("1", ps.condition.value);
    }

    @Test
    void parseDeleteWithoutWhere() {
        ParsedStatement ps = parser.parse("DELETE FROM users");

        assertEquals(StatementType.DELETE, ps.statementType);
        assertEquals("users", ps.tableName);
        assertNull(ps.condition);
    }

    @Test
    void parseUnknownStatementThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse("DROP TABLE users"));
    }

    @Test
    void parseSelectWithGreaterThanCondition() {
        ParsedStatement ps = parser.parse("SELECT * FROM users WHERE id > 5");

        assertNotNull(ps.condition);
        assertEquals(">", ps.condition.operator);
        assertEquals("5", ps.condition.value);
    }
}
