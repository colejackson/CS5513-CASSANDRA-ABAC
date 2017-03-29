package org.apache.cassandra.cql3;

import java.io.Serializable;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyClause implements Serializable
{
    public static final String WHERE_PLACEHOLDER = "';;;'";

    private final String attribute;

    private final String attributeTypeString;

    private final String opString;

    private final String colIdString;

    public PolicyClause(String attribute, CQL3Type nativeType, Operator op, ColumnIdentifier colId)
    {
        this.attribute = attribute;

        this.attributeTypeString = nativeType.toString();

        this.opString = op.toString();

        this.colIdString = colId.toCQLString();
    }

    @Override
    public String toString()
    {
        return String.format("Attribute [%s] %s value is %s column %s.",
                             attribute,
                             attributeTypeString,
                             opString,
                             colIdString);
    }

    public String generateWhereClause()
    {
        return null; // TODO: ABAC produce the clause that will be added to the Query for this rule.
    }

    public AbstractType getAttributeType()
    {
        return CQL3Type.Native.match(attributeTypeString).getType();
    }

    public String getAttributeName()
    {
        return attribute;
    }
}
