package org.apache.cassandra.auth;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.relations.PolicyRelation;
import org.apache.cassandra.utils.Pair;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.cassandra.auth.AbacProxy.getAllPoliciesOn;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyCache extends AuthCache<Pair<String,String>, Set<PolicyRelation>>
{
    public PolicyCache() // TODO: ABAC Update to not use the permissions cache methods.
    {
        super("PolicyCache",
                DatabaseDescriptor::setPermissionsValidity,
                DatabaseDescriptor::getPermissionsValidity,
                DatabaseDescriptor::setPermissionsUpdateInterval,
                DatabaseDescriptor::getPermissionsUpdateInterval,
                DatabaseDescriptor::setPermissionsCacheMaxEntries,
                DatabaseDescriptor::getPermissionsCacheMaxEntries,
                (p) -> getAllPoliciesOn(p.left, p.right),
                DatabaseDescriptor::isUsingAbac);
    }

    public Set<PolicyRelation> getPolicies(String tableName, String permString)
    {


        try
        {
            return get(Pair.create(tableName, permString));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
