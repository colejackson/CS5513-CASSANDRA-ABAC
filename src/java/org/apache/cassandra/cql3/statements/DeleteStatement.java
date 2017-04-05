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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.auth.AbacProxy;
import org.apache.cassandra.auth.Attribute;
import org.apache.cassandra.auth.AttributeValue;
import org.apache.cassandra.auth.Policy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.relations.PolicyRelation;
import org.apache.cassandra.cql3.relations.Relation;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 */
public class DeleteStatement extends ModificationStatement
{
    private DeleteStatement(int boundTerms,
                            TableMetadata cfm,
                            Operations operations,
                            StatementRestrictions restrictions,
                            Conditions conditions,
                            Attributes attrs)
    {
        super(StatementType.DELETE, boundTerms, cfm, operations, restrictions, conditions, attrs);
    }

    @Override
    public void addUpdateForKey(PartitionUpdate update, Clustering clustering, UpdateParameters params)
    throws InvalidRequestException
    {
        TableMetadata metadata = metadata();

        List<Operation> regularDeletions = getRegularOperations();
        List<Operation> staticDeletions = getStaticOperations();

        if (regularDeletions.isEmpty() && staticDeletions.isEmpty())
        {
            // We're not deleting any specific columns so it's either a full partition deletion ....
            if (clustering.size() == 0)
            {
                update.addPartitionDeletion(params.deletionTime());
            }
            // ... or a row deletion ...
            else if (clustering.size() == metadata.clusteringColumns().size())
            {
                params.newRow(clustering);
                params.addRowDeletion();
                update.add(params.buildRow());
            }
            // ... or a range of rows deletion.
            else
            {
                update.add(params.makeRangeTombstone(metadata.comparator, clustering));
            }
        }
        else
        {
            if (!regularDeletions.isEmpty())
            {
                // if the clustering size is zero but there are some clustering columns, it means that it's a
                // range deletion (the full partition) in which case we need to throw an error as range deletion
                // do not support specific columns
                checkFalse(clustering.size() == 0 && metadata.clusteringColumns().size() != 0,
                           "Range deletions are not supported for specific columns");

                params.newRow(clustering);

                for (Operation op : regularDeletions)
                    op.execute(update.partitionKey(), params);
                update.add(params.buildRow());
            }

            if (!staticDeletions.isEmpty())
            {
                params.newRow(Clustering.STATIC_CLUSTERING);
                for (Operation op : staticDeletions)
                    op.execute(update.partitionKey(), params);
                update.add(params.buildRow());
            }
        }
    }

    @Override
    public void addUpdateForKey(PartitionUpdate update, Slice slice, UpdateParameters params)
    {
        List<Operation> regularDeletions = getRegularOperations();
        List<Operation> staticDeletions = getStaticOperations();

        checkTrue(regularDeletions.isEmpty() && staticDeletions.isEmpty(),
                  "Range deletions are not supported for specific columns");

        update.add(params.makeRangeTombstone(slice));
    }

    public static class Parsed extends ModificationStatement.Parsed
    {
        private final List<Operation.RawDeletion> deletions;
        protected WhereClause whereClause;

        public Parsed(CFName name,
                      Attributes.Raw attrs,
                      List<Operation.RawDeletion> deletions,
                      WhereClause whereClause,
                      List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions,
                      boolean ifExists)
        {
            super(name, StatementType.DELETE, attrs, conditions, false, ifExists);
            this.deletions = deletions;
            this.whereClause = whereClause;
        }


        @Override
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            Operations operations = new Operations(type);

            for (Operation.RawDeletion deletion : deletions)
            {
                ColumnMetadata def = getColumnDefinition(metadata, deletion.affectedColumn());

                // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
                // list. However, we support having the value name for coherence with the static/sparse case
                checkFalse(def.isPrimaryKeyColumn(), "Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", def.name);

                Operation op = deletion.prepare(metadata.keyspace, def, metadata);
                op.collectMarkerSpecification(boundNames);
                operations.add(op);
            }

            StatementRestrictions restrictions = newRestrictions(metadata,
                                                                 boundNames,
                                                                 operations,
                                                                 whereClause,
                                                                 conditions);

            DeleteStatement stmt = new DeleteStatement(boundNames.size(),
                                                       metadata,
                                                       operations,
                                                       restrictions,
                                                       conditions,
                                                       attrs);

            if (stmt.hasConditions() && !restrictions.hasAllPKColumnsRestrictedByEqualities())
            {
                checkFalse(operations.appliesToRegularColumns(),
                           "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns");

                // All primary keys must be specified, unless this has static column restrictions
                checkFalse(conditions.appliesToRegularColumns(),
                           "DELETE statements must restrict all PRIMARY KEY columns with equality relations" +
                           " in order to use IF condition on non static columns");
            }

            return stmt;
        }

        public void prepareAbac()
        {
            List<Policy> policies = null;

            if(DatabaseDescriptor.isUsingAbac())
            {
                policies = AbacProxy.getPolicies(cfName, "MODIFY");
            }

            if(policies != null && !policies.isEmpty())
            {
                WhereClause.Builder builder = new WhereClause.Builder();

                whereClause.relations.forEach(builder::add);
                policies.forEach(p -> p.getRelations().forEach(builder::add));

                whereClause = builder.build();
            }
        }

        public boolean requiresAttributes()
        {
            for(Relation relation : whereClause.relations)
            {
                if(relation instanceof PolicyRelation)
                {
                    return true;
                }
            }

            return false;
        }

        public Set<Attribute> getRequiredAttributes()
        {
            Set<Attribute> attributes = new HashSet<>();

            for(Relation relation : whereClause.relations)
            {
                if(relation instanceof PolicyRelation)
                {
                    attributes.add(((PolicyRelation) relation).getExpectedAttribute());
                }
            }

            return attributes;
        }

        public void setRequiredAttributes(Set<AttributeValue> attributed)
        {
            for(Relation relation : whereClause.relations)
            {
                if(relation instanceof PolicyRelation)
                {
                    Attribute attribute = ((PolicyRelation) relation).getExpectedAttribute();

                    if(attributed.stream().noneMatch(a -> a.toAttribute().equivalentTo(attribute)))
                    {
                        throw new AssertionError("Something should match here, make sure it does.");
                    }

                    int i = 0;
                    Term.Raw term = null;

                    for(AttributeValue attr : attributed)
                    {
                        if(attr.toAttribute().equivalentTo(attribute))
                        {
                            term = attr.value;
                        }
                    }

                    ((PolicyRelation) relation).setValue(term);
                }
            }
        }
    }
}
