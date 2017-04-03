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

import org.apache.cassandra.auth.Attribute;
import org.apache.cassandra.cql3.Term;

/**
 * Created by coleman on 4/2/17.
 */
public class AttributeValue
{
    public final Term.Raw value;

    public final String attributeName;

    public AttributeValue(String attributeName, Term.Raw value)
    {
        this.attributeName = attributeName;

        this.value = value;
    }

    public boolean compatibleWith(Attribute otherAttribute, String keyspaceName)
    {
        if(this.attributeName != null
           && otherAttribute.attributeName != null
           && !this.attributeName.equalsIgnoreCase(otherAttribute.attributeName))
        {
            return false;
        }

        if(this.value != null
           && otherAttribute.attributeType != null
           && !otherAttribute.attributeType.getType().isCompatibleWith(this.value.getExactTypeIfKnown(keyspaceName)))
        {
            return false;
        }

        return true;
    }

    public Attribute toAttribute()
    {
        return Attribute.getBuilder().setName(attributeName).build();
    }
}
