package org.apache.cassandra.auth;

import org.apache.cassandra.cql3.PolicyClause;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by coleman on 3/26/17.
 */
public final class AbacProxy
{
    // TODO: ABAC Test

    public static void createPolicy()
    {

    }

    public static void dropPolicy()
    {
    }

    public static void alterPolicy()
    {

    }

    static ResultSet listAllPolicies()
    {
        return null;
    }

    static Set<PolicyClause> listAllPoliciesOn(IResource resource, Permission permission)
    {
        Set<PolicyClause> ret = new HashSet<>();

        String cqlString = String.format("SELECT obj FROM %s.%s WHERE columnfamily = %s AND type = %s",
                SchemaConstants.AUTH_KEYSPACE_NAME,
                AuthKeyspace.POLICIES,
                resource.getName(),
                permission.toString());

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
}
