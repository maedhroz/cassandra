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

package org.apache.cassandra.utils.ast;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

public interface Value extends ReferenceExpression
{
    static Gen<Value> gen(Object value, AbstractType<?> type)
    {
        //TODO switch back, trying to make easier to read queries for debugging
        return ignore -> new Bind(value, type);
//        Gen<Boolean> bool = SourceDSL.booleans().all();
//        return rnd -> {
//            return bool.generate(rnd) ?
//                   new Bind(value, type)
//                   : new Literal(value, type);
//        };
    }

    static Gen<Value> gen(AbstractType<?> type)
    {
        Gen<?> v = AbstractTypeGenerators.getTypeSupport(type).valueGen;
        return rnd -> Value.gen(v.generate(rnd), type).generate(rnd);
    }
}
