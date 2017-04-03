package org.apache.cassandra.auth;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.relations.PolicyRelation;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.w3c.dom.Attr;

import com.google.common.collect.ImmutableList;

import javax.xml.bind.DatatypeConverter;
import javax.xml.transform.Result;
import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by coleman on 3/26/17.
 */
public final class AbacProxy
{
    public static void createPolicy(Policy policy) {}

    public static void dropPolicy(Policy policy) {}

    public static ResultMessage listPolicies(TableMetadata table) {return null;}

    public static List<Policy> getPolicies(TableMetadata table) {return null;}

    public static boolean policyExists(Policy policy) {return false;}

    public static void createAttribute(Attribute attribute) {}

    public static void dropAttribute(Attribute attribute) {}

    public static ResultMessage listAttributes() {return null;}

    public static Attribute getAttributes(Attribute attribute) {return null;}

    public static boolean attributeExists(Attribute attribute) {return false;}

    public static void createPolicy(String policyName,
                                    String cfName,
                                    Set<Permission> perms,
                                    PolicyRelation policy)
    {
        String perm;

        if(perms.size() > 1)
        {
            perm = "ALL";
        }
        else if(perms.contains(Permission.SELECT))
        {
            perm = "SELECT";
        }
        else
        {
            perm = "MODIFY";
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try
        {
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(policy);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to stream object output.");
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
                escape(perm));

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

    public static Set<PolicyRelation> getAllPoliciesOn(String tableName, String permissionString)
    {
        Set<PolicyRelation> ret = new HashSet<>();

        String cqlString = String.format("SELECT obj, type FROM %s.%s WHERE cf = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                escape(tableName));

        UntypedResultSet results = QueryProcessor.process(cqlString, ConsistencyLevel.LOCAL_ONE);

        for(UntypedResultSet.Row r : results)
        {
            try(ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(r.getBlob("obj").array())))
            {
                if("ALL".equalsIgnoreCase(permissionString) || r.getString("type").equalsIgnoreCase(permissionString))
                {
                    ret.add((PolicyRelation) ois.readObject());
                }
            }
            catch (IOException | ClassNotFoundException c)
            {
                throw new RuntimeException("Problem with deserializing policies.");
            }
        }

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
        ResultSet results = new ResultSet(POLICY_SPECIFICATION);

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

    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String POLICY_CF = AuthKeyspace.POLICIES;

    private static final List<ColumnSpecification> POLICY_SPECIFICATION =
    ImmutableList.of(
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("policy", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("cf", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("description", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("type", true), UTF8Type.instance));

    public static Function<UntypedResultSet.Row, Policy> ROW_TO_POLICY = new Function<UntypedResultSet.Row, Policy>()
    {
        public Policy apply(UntypedResultSet.Row row)
        {


            return null;
        }
    };

    private static final String ATTRIBUTE_CF = AuthKeyspace.ATTRIBUTES;

    private static final List<ColumnSpecification> ATTRIBUTE_SPECIFICATION =
    ImmutableList.of(
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("policy", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("cf", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("description", true), UTF8Type.instance),
    new ColumnSpecification(KS, POLICY_CF, new ColumnIdentifier("type", true), UTF8Type.instance));

    public static Function<UntypedResultSet.Row, Attribute> ROW_TO_ATTRIBUTE = new Function<UntypedResultSet.Row, Attribute>()
    {
        public Attribute apply(UntypedResultSet.Row row)
        {


            return null;
        }
    };
}
