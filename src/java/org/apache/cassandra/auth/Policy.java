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

package org.apache.cassandra.auth;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.relations.Relation;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Created by coleman on 3/30/17.
 */
public class Policy implements Serializable
{
    public final String policyName;

    public final CFName columnFamily;

    public final WhereClause whereClause;

    public final Set<Permission> permission;

    public Policy(String policyName, CFName columnFamily, WhereClause whereClause, Set<Permission> perm)
    {
        this.policyName = policyName;

        this.columnFamily = columnFamily;

        this.whereClause = whereClause;

        this.permission = perm;
    }

    public List<Relation> getRelations()
    {
        return whereClause.relations;
    }

    public boolean isEquivalent(Policy otherPolicy)
    {
        if(this.policyName != null
           && otherPolicy.policyName != null
           && !this.policyName.equalsIgnoreCase(otherPolicy.policyName))
        {
            return false;
        }

        if(this.columnFamily != null
           && otherPolicy.columnFamily != null
           && !this.columnFamily.equals(otherPolicy.columnFamily))
        {
            return false;
        }

        if(this.whereClause != null
           && otherPolicy.whereClause != null
           && !this.whereClause.equals(otherPolicy.whereClause))
        {
            return false;
        }

        if(this.permission != null
           && otherPolicy.permission != null
           && this.permission.equals(otherPolicy.permission))
        {
            return false;
        }

        return false;
    }
}
