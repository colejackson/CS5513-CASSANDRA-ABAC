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

import org.apache.cassandra.cql3.CQL3Type;

/**
 * Created by coleman on 3/30/17.
 */
public class Attribute implements Serializable
{
    public final String attributeName;
    public final CQL3Type attributeType;

    private Attribute(String attributeName, CQL3Type attributeType)
    {
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }

    public static Builder getBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String attributeName;
        private CQL3Type attributeType;

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

        public Attribute build()
        {
            return new Attribute(attributeName, attributeType);
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

        return true;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Attribute attribute = (Attribute) o;

        if (attributeName != null ? !attributeName.equals(attribute.attributeName) : attribute.attributeName != null)
            return false;
        if (attributeType != null ? !attributeType.equals(attribute.attributeType) : attribute.attributeType != null)
            return false;

        return true;
    }

    public int hashCode()
    {
        int result = attributeName != null ? attributeName.hashCode() : 0;
        result = 31 * result + (attributeType != null ? attributeType.hashCode() : 0);
        return result;
    }
}
