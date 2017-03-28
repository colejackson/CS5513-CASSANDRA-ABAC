package org.apache.cassandra.auth;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.messages.ResultMessage;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by coleman on 3/26/17.
 */
public final class AbacProxy
{
    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String CF = AuthKeyspace.POLICIES;

    private static final List<ColumnSpecification> metadata =
            ImmutableList.of(
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("policy", true), UTF8Type.instance),
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("columnfamily", true), UTF8Type.instance),
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("description", true), UTF8Type.instance),
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("obj", true), BytesType.instance),
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("type", true), UTF8Type.instance));

    public static void createPolicy(String policyName, String cfName, Set<Permission> perms, PolicyClause policy)
    {
        String type;

        if(perms.size() > 1)
        {
            type = "ALL";
        }
        else if(perms.contains(Permission.SELECT))
        {
            type = "SELECT";
        }
        else
        {
            type = "MODIFY";
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try
        {
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(policy);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        String cqlString = String.format("INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s)",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape("policy"),
                escape("columnFamily"),
                escape("description"),
                escape("obj"),
                escape("type"),
                escape(policyName),
                escape(cfName),
                escape(policy.toString()),
                DatatypeConverter.printHexBinary(out.toByteArray()),
                escape(type));

        QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);
    }

    public static void dropPolicy(String columnFamilyName, String policyName)
    {
        String cqlString = String.format("DELETE FROM %s.%s WHERE columnfamily = %s AND policy = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(columnFamilyName),
                escape(policyName));

        QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);
    }

    public static boolean policyExists(String columnFamilyName, String policyName)
    {
        String cqlString = String.format("SELECT obj FROM %s.%s WHERE columnfamily = %s AND policy = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(columnFamilyName),
                escape(policyName)
                );

        UntypedResultSet results = QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);

        return !results.isEmpty();
    }

    static Set<PolicyClause> listAllPoliciesOn(IResource resource, Permission permission)
    {
        Set<PolicyClause> ret = new HashSet<>();

        String cqlString = String.format("SELECT obj FROM %s.%s WHERE columnfamily = %s AND type = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(resource.getName()),
                escape(permission.toString()));

        UntypedResultSet results = QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);

        results.forEach((UntypedResultSet.Row r) ->
        {
            try
            {
                ret.add((PolicyClause) (new ObjectInputStream(new ByteArrayInputStream(r.getBlob("obj").array())).readObject()));
            }
            catch (IOException | ClassNotFoundException c)
            {
                c.printStackTrace();
            }
        });

        return ret;
    }

    public static ResultMessage listAllPoliciesOnTable(String tableName)
    {
        String cqlString = String.format("SELECT obj FROM %s.%s WHERE columnfamily = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(tableName));

        UntypedResultSet results = QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);

        return prepare(results);
    }

    private static ResultMessage prepare(UntypedResultSet untypedResultSet)
    {
        ResultSet results = new ResultSet(metadata);

        untypedResultSet.forEach(row -> {
            results.addColumnValue(row.getBytes("policy"));
            results.addColumnValue(row.getBytes("columnfamily"));
            results.addColumnValue(row.getBytes("description"));
            results.addColumnValue(row.getBlob("obj"));
            results.addColumnValue(row.getBytes("type"));
        });

        return new ResultMessage.Rows(results);
    }

    private static String escape(String str)
    {
        return "'" + str.replace("'", "''") + "'";
    }
}
