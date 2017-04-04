package org.apache.cassandra.auth;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.messages.ResultMessage;

public final class AbacProxy
{
    public static void createPolicy(Policy policy)
    {
        String cqlQuery;

        try
        {
            cqlQuery = String.format("INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (%s, %s, %s, %s)",
                                            SchemaConstants.AUTH_KEYSPACE_NAME,
                                            AuthKeyspace.POLICIES,
                                            "policy",
                                            "cf",
                                            "obj",
                                            "permissions",
                                            escape(policy.policyName),
                                            escape(policy.columnFamily.toString()),
                                            "0x" + toString(policy),
                                            escape(policy.permission.size() > 1 ? "ALL" :
                                                   policy.permission.contains(Permission.SELECT) ? "SELECT" :
                                                   "MODIFY"));
        }
        catch (IOException e)
        {
            throw new InvalidRequestException("The policy object could not be serialized, try again.");
        }

        QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);
    }

    public static void dropPolicy(Policy policy)
    {
        String cqlQuery = String.format("DELETE FROM %s.%s WHERE policy = %s AND cf = %s",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.POLICIES,
                                        escape(policy.policyName),
                                        escape(policy.columnFamily.toString()));

        QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);
    }

    public static ResultMessage listPolicies(CFName table)
    {
        List<Policy> policies = getPolicies(table, "ALL");

        ResultSet result = new ResultSet(POLICY_SPECIFICATION);
        for (Policy policy : policies)
        {
            result.addColumnValue(UTF8Type.instance.decompose(policy.policyName));
            result.addColumnValue(UTF8Type.instance.decompose(policy.columnFamily.toString()));
            result.addColumnValue(UTF8Type.instance.decompose(policy.permission.size() > 1 ? "ALL" :
                                                              policy.permission.contains(Permission.SELECT) ? "SELECT" :
                                                              "MODIFY"));
        }
        return new ResultMessage.Rows(result);
    }

    public static List<Policy> getPolicies(CFName cfname, String perm)
    {
        if(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(cfname.getKeyspace()) ||
           SchemaConstants.SYSTEM_KEYSPACE_NAMES.contains(cfname.getKeyspace()))
        {
            return ImmutableList.<Policy>builder().build();
        }

        String cqlQuery = String.format("SELECT obj FROM %s.%s WHERE cf = %s AND permissions IN ('ALL', %s)",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.POLICIES,
                                        escape(cfname.toString()),
                                        escape(perm));

        UntypedResultSet results = QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);

        Iterable<Policy> policies = Iterables.transform(results, row -> fromBytes(row != null ? row.getBlob("obj") : null));
        return ImmutableList.<Policy>builder().addAll(policies).build();
    }

    public static boolean policyExists(Policy policy)
    {
        String cqlQuery = String.format("SELECT * FROM %s.%s WHERE policy = %s AND cf = %s",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.POLICIES,
                                        escape(policy.policyName),
                                        escape(policy.columnFamily.toString()));

        UntypedResultSet results = QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);

        return !results.isEmpty();
    }

    public static void createAttribute(Attribute attribute)
    {
        String cqlQuery = String.format("INSERT INTO %s.%s (%s, %s) VALUES (%s, %s)",
                                 SchemaConstants.AUTH_KEYSPACE_NAME,
                                 AuthKeyspace.ATTRIBUTES,
                                 "attribute",
                                 "type",
                                 escape(attribute.attributeName),
                                 escape(attribute.attributeType.toString()));

        QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);
    }

    public static void dropAttribute(Attribute attribute)
    {
        String cqlQuery = String.format("DELETE FROM %s.%s WHERE attribute = %s",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.ATTRIBUTES,
                                        escape(attribute.attributeName));

        QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);
    }

    public static ResultMessage listAttributes()
    {
        String cqlQuery = String.format("SELECT * FROM %s.%s",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.ATTRIBUTES);

        UntypedResultSet results = QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);

        ResultSet result = new ResultSet(ATTRIBUTE_SPECIFICATION);
        for (UntypedResultSet.Row row : results)
        {
            result.addColumnValue(UTF8Type.instance.decompose(row.getString("attribute")));
            result.addColumnValue(UTF8Type.instance.decompose(row.getString("type")));
        }
        return new ResultMessage.Rows(result);
    }

    static Attribute getAttribute(Attribute attribute)
    {
        String cqlQuery = String.format("SELECT * FROM %s.%s WHERE attribute = %s",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.ATTRIBUTES,
                                        escape(attribute.attributeName));

        UntypedResultSet results = QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);

        return ROW_TO_ATTRIBUTE.apply(results.one());
    }

    public static boolean attributeExists(Attribute attribute)
    {
        String cqlQuery = String.format("SELECT * FROM %s.%s WHERE attribute = %s",
                                        SchemaConstants.AUTH_KEYSPACE_NAME,
                                        AuthKeyspace.ATTRIBUTES,
                                        escape(attribute.attributeName));

        UntypedResultSet results = QueryProcessor.process(cqlQuery, ConsistencyLevel.LOCAL_ONE);

        return !results.isEmpty();
    }

    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String POLICY_CF = AuthKeyspace.POLICIES;

    private static final List<ColumnSpecification> POLICY_SPECIFICATION =
    ImmutableList.of(
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("policy", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("cf", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("permissions", true), UTF8Type.instance));

    private static final String ATTRIBUTE_CF = AuthKeyspace.ATTRIBUTES;

    private static final List<ColumnSpecification> ATTRIBUTE_SPECIFICATION =
    ImmutableList.of(
    new ColumnSpecification(KS, ATTRIBUTE_CF, new ColumnIdentifier("attribute", true), UTF8Type.instance),
    new ColumnSpecification(KS, ATTRIBUTE_CF, new ColumnIdentifier("type", true), UTF8Type.instance));

    private static final Function<UntypedResultSet.Row, Attribute> ROW_TO_ATTRIBUTE = row -> Attribute.getBuilder()
                                                                                                      .setName(row.getString("attribute"))
                                                                                                      .setType(CQL3Type.Native.match(row.getString("type")))
                                                                                                      .build();

    private static final Pattern COMPILE = Pattern.compile("'", Pattern.LITERAL);

    private static String escape(String input)
    {
        return '\'' + COMPILE.matcher(input).replaceAll(Matcher.quoteReplacement("''")) + '\'';
    }

    private static Policy fromBytes(ByteBuffer b)
    {
        Object o;

        try
        {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b.array()));
            o  = ois.readObject();
            ois.close();
        }
        catch(Exception e)
        {
            throw new InvalidRequestException("Couldn't deserialize the policy object.");
        }

        return (Policy)o;
    }

    private static String toString(Policy p) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(p);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}
