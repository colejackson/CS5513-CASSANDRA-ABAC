/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.AbacProxy;
import org.apache.cassandra.auth.AttributeValue;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * Created by coleman on 4/2/17.
 */
public class RemoveAttributeStatement extends AuthenticationStatement
{
    private final AttributeValue attribute;

    private final RoleResource role;

    public RemoveAttributeStatement(AttributeValue attribute, RoleName role)
    {
        this.attribute = attribute;

        this.role = RoleResource.role(role.getName());
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        AuthenticatedUser user = state.getUser();

        if(user.isSuper())
        {
            return;
        }

        state.ensureHasPermission(Permission.ALTER, role);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        state.ensureNotAnonymous();

        if(!DatabaseDescriptor.getRoleManager().isExistingRole(role))
        {
            throw new InvalidRequestException(String.format("Role {%s} does not exist.", role.getRoleName()));
        }

        if(!AbacProxy.attributeExists(attribute.toAttribute()))
        {
            throw new InvalidRequestException(String.format("Attribute {%s} does not exist.", attribute.attributeName));
        }

        if(!DatabaseDescriptor.getRoleManager().hasAttribute(attribute, role))
        {
            throw new InvalidRequestException(String.format("Attribute {%s} is undefined on role {%s}",
                                                            attribute.attributeName,
                                                            role.getRoleName()));
        }
    }

    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        DatabaseDescriptor.getRoleManager().removeAttribute(role, attribute);

        return null;
    }
}
