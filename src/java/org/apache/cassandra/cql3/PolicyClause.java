package org.apache.cassandra.cql3;

import java.io.Serializable;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyClause implements Serializable
{
    private final String attribute;

    private transient Operator operator;
    private String opString;

    private transient ColumnIdentifier colId;
    private final String colIdString;

    private boolean containsNullLiteral = false;

    public PolicyClause(String attribute, Operator op, ColumnIdentifier colId)
    {
        this.attribute = attribute;

        this.operator = op;
        this.opString = this.operator.toString();

        this.colId = colId;
        this.colIdString = this.colId.toCQLString();
    }

    public PolicyClause(String attribute, Operator op)
    {
        this.attribute = attribute;

        this.operator = op;
        this.opString = this.operator.toString();

        this.containsNullLiteral = true;
        this.colIdString = this.colId.toCQLString();
    }

    public PolicyClause(String attribute, ColumnIdentifier colId)
    {
        this.attribute = attribute;

        this.colId = colId;
        this.colIdString = this.colId.toString();
    }

    @Override
    public String toString()
    {
        if(operator == Operator.CONTAINS)
        {
            return String.format("Column value %s contains attribute %s value.", colIdString, attribute);
        }
        else if(operator == null)
        {
            return String.format("Attribute %s value is in column %s", attribute, colIdString);
        }
        else if(containsNullLiteral)
        {
            return String.format("Attribute %s value is not null", attribute);
        }
        else
        {
            return String.format("Column value %s is %s attribute %s value", colIdString, opString, attribute);
        }
    }

    public String generateWhereClause()
    {
        return null; // TODO: ABAC produce the clause that will be added to the Query for this rule.
    }
}
