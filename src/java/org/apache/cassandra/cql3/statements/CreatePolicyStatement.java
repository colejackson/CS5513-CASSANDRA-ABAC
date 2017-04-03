package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.AbacProxy;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.Policy;
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
    private final Policy policy;
    private final CFName cfName;

    public CreatePolicyStatement(Policy policy, CFName cfname)
    {
        super(cfname);

        this.policy = policy;
        this.cfName = cfname;
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

        if(AbacProxy.policyExists(policy))
        {
            throw new InvalidRequestException(String.format("A policy {%s} already exists on table {%s}",
                                                            policy.policyName,
                                                            cfName.getColumnFamily()));
        }
    }

    @Override
    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        AbacProxy.createPolicy(policy);

        return null;
    }
}
