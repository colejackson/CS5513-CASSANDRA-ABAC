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

import org.apache.cassandra.cql3.CQL3Type;

/**
 * Created by coleman on 3/30/17.
 */
public class Attribute
{
    public final String attributeName;
    public final CQL3Type attributeType;
    public final AttributeOrdering attributeOrdering;

    private Attribute(String attributeName, CQL3Type attributeType, AttributeOrdering attributeOrdering)
    {
        this.attributeName = attributeName;
        this.attributeType = attributeType;
        this.attributeOrdering = attributeOrdering;
    }

    public static Builder getBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String attributeName;
        private CQL3Type attributeType;
        private AttributeOrdering attributeOrdering;

        private Builder() {}

        public Builder setName(String name)
        {
            this.attributeName = name;
            return this;
        }

        public Builder setType(CQL3Type type)
        {
            this.attributeType = type;
            return this;
        }

        public Builder setOrdering(AttributeOrdering ordering)
        {
            this.attributeOrdering = ordering;
            return this;
        }

        public Attribute build()
        {
            return new Attribute(attributeName, attributeType, attributeOrdering);
        }
    }

    public boolean equivalentTo(Attribute otherAttribute)
    {
        if(this.attributeName != null
            && otherAttribute.attributeName != null
            && !this.attributeName.equalsIgnoreCase(otherAttribute.attributeName))
        {
            return false;
        }

        if(this.attributeType != null
           && otherAttribute.attributeType != null
           && this.attributeType != otherAttribute.attributeType)
        {
            return false;
        }

        if(this.attributeOrdering != null
           && otherAttribute.attributeOrdering != null
           && !this.attributeOrdering.equals(otherAttribute.attributeOrdering))
        {
            return false;
        }

        return true;
    }
}
