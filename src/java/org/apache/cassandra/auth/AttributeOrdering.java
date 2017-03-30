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

import java.util.Comparator;

import org.apache.cassandra.cql3.CQL3Type;

/**
 * Created by coleman on 3/30/17.
 */
public class AttributeOrdering
{
    private Comparator comparator;

    public enum Kind
    {
        STANDARD, REVERSE, CUSTOM
    }

    private AttributeOrdering(CQL3Type type, Kind kind)
    {
        comparator = null;
    }

    private AttributeOrdering(CQL3Type type, Kind kind, Class customClass)
    {
        comparator = null;
    }

    private void setComparator(CQL3Type type, Kind kind)
    {
        this.comparator = null;
    }

    public Comparator getComparator()
    {
        return this.comparator;
    }

    public Builder getBuilder()
    {
        return new AttributeOrdering.Builder();
    }

    public static class Builder
    {
        private CQL3Type type;
        private Kind kind;
        private Class<?> customClass;

        public Builder setType(CQL3Type type)
        {
            this.type = type;
            return this;
        }

        public Builder setKind(Kind kind)
        {
            this.kind = kind;
            return this;
        }

        public Builder setCustomClass(String className) throws ClassNotFoundException
        {
            this.customClass = Class.forName(className);
            return this;
        }

        public AttributeOrdering build() throws Exception
        {
            if(type == null || kind == null)
            {
                throw new Exception("Cannot build attribute ordering without kind and type declared");
            }

            if(!customClass.isInstance(Comparator.class))
            {
                throw new Exception("Custom class must implement Comparator");
            }

            if(customClass == null)
            {
                return new AttributeOrdering(type, kind);
            }
            else
            {
                return new AttributeOrdering(type, kind, customClass);
            }
        }
    }
}
