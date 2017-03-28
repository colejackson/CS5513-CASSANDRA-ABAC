package org.apache.cassandra.auth;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("cf", true), UTF8Type.instance),
                    new ColumnSpecification(KS, CF, new ColumnIdentifier("description", true), UTF8Type.instance),
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
                "policy",
                "cf",
                "description",
                "obj",
                "type",
                escape(policyName),
                escape(cfName),
                escape(policy.toString()),
                "0x" + DatatypeConverter.printHexBinary(out.toByteArray()),
                escape(type));

        QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);
    }

    public static void dropPolicy(String columnFamilyName, String policyName)
    {
        String cqlString = String.format("DELETE FROM %s.%s WHERE cf = %s AND policy = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(columnFamilyName),
                escape(policyName));

        QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);
    }

    public static boolean policyExists(String columnFamilyName, String policyName)
    {
        String cqlString = String.format("SELECT obj FROM %s.%s WHERE cf = %s AND policy = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(columnFamilyName),
                escape(policyName)
                );

        UntypedResultSet results = QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);

        return !results.isEmpty();
    }

    static Set<PolicyClause> getAllPoliciesOn(String tableName, String permissionString)
    {
        Set<PolicyClause> ret = new HashSet<>();

        String cqlString = String.format("SELECT obj FROM %s.%s WHERE cf = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(tableName));

        UntypedResultSet results = QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);

        results.forEach((UntypedResultSet.Row r) ->
        {
            try
            {
                if(r.getString("type").equalsIgnoreCase(permissionString))
                {
                    ret.add((PolicyClause) (new ObjectInputStream(new ByteArrayInputStream(r.getBlob("obj").array())).readObject()));
                }
            }
            catch (IOException | ClassNotFoundException c)
            {
                throw new RuntimeException("Problem with deserializing policies.");
            }
        });

        return ret;
    }

    public static ResultMessage listAllPoliciesOnTable(String tableName)
    {
        String cqlString = String.format("SELECT * FROM %s.%s WHERE cf = %s",
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
            results.addColumnValue(row.getBytes("cf"));
            results.addColumnValue(row.getBytes("description"));
            results.addColumnValue(row.getBytes("type"));
        });

        return new ResultMessage.Rows(results);
    }

    private static String escape(String str)
    {
        return '\'' + str.replace("'", "''") + '\'';
    }
}
