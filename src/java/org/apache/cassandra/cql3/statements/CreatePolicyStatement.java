package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.AbacProxy;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.relations.PolicyRelation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.util.Set;

/**
 * Created by coleman on 3/27/17.
 */
public class CreatePolicyStatement extends AbacStatement
{
    private final PolicyName policyName;
    private final CFName cfName;
    private final Set<Permission> perms;
    private final PolicyRelation policyRelation;

    public CreatePolicyStatement(PolicyName policyName,
                                 CFName cfname,
                                 Set<Permission> perms,
                                 PolicyRelation policyRelation)
    {
        super(cfname);

        this.policyName = policyName;
        this.cfName = cfname;
        this.perms = perms;
        this.policyRelation = policyRelation;
    }

    @Override
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        AuthenticatedUser user = state.getUser();
        boolean isSuper = user.isSuper();

        // superusers can do whatever else they like
        if (isSuper)
            return;

        if (!cfName.hasKeyspace())
            cfName.setKeyspace(keyspace(), true);

        state.hasColumnFamilyAccess(keyspace(), cfName.getColumnFamily(), Permission.CREATE);
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        state.ensureNotAnonymous();

        if(AbacProxy.policyExists(cfName.getKeyspace() + '.' + cfName.getColumnFamily(), policyName.getName()))
        {
            throw new InvalidRequestException("A policy with this name already exists");
        }
    }

    @Override
    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        AbacProxy.createPolicy(policyName.getName(),
                               cfName.getKeyspace() + "." + cfName.getColumnFamily(),
                               perms,
                               policyRelation);

        return null;
    }
}
