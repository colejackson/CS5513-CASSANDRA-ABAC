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
    // TODO: ABAC IMPLEMENT PERSISTING THESE OBJECTS

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
