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

package org.apache.cassandra.cql3.relations;

import java.util.List;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyRelation extends Relation
{
    private final Relation relation;

    private final String expectedAttribute;

    private boolean valueIsSet = false;

    public PolicyRelation(Relation relation, String expectedAttribute)
    {
        this.relation = relation;

        this.expectedAttribute = expectedAttribute;
    }

    public boolean isMultiColumn()
    {
        return relation.isMultiColumn();
    }

    public Operator operator()
    {
        return relation.operator();
    }

    public boolean onToken()
    {
        return relation.onToken();
    }

    protected void setValue(Term.Raw term)
    {
        this.relation.setValue(term);

        valueIsSet = true;
    }

    public Term.Raw getValue()
    {
        validate();

        return relation.getValue();
    }

    public List<? extends Term.Raw> getInValues()
    {
        validate();

        return relation.getInValues();
    }

    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        validate();

        return relation.newEQRestriction(table, boundNames);
    }

    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        validate();

        return relation.newINRestriction(table, boundNames);
    }

    protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive)
    {
        validate();

        return relation.newSliceRestriction(table, boundNames, bound, inclusive);
    }

    protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        validate();

        return relation.newContainsRestriction(table, boundNames, isKey);
    }

    protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        validate();

        return relation.newIsNotRestriction(table, boundNames);
    }

    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        validate();

        return relation.newLikeRestriction(table, boundNames, operator);
    }

    protected Term toTerm(List<? extends ColumnSpecification> receivers, Term.Raw raw, String keyspace, VariableSpecifications boundNames)
    {
        validate();

        return relation.toTerm(receivers, raw, keyspace, boundNames);
    }

    public Relation renameIdentifier(ColumnMetadata.Raw from, ColumnMetadata.Raw to)
    {
        validate();

        return relation.renameIdentifier(from, to);
    }

    private void validate()
    {
        if(!valueIsSet)
        {
            throw new AssertionError("Policy Relation Value should always be set.");
        }
    }

    public String getExpectedAttribute()
    {
        return this.expectedAttribute;
    }
}
