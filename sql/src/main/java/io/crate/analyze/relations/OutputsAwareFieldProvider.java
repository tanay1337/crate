/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import com.google.common.collect.Multimap;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;

public final class OutputsAwareFieldProvider implements FieldProvider<Symbol> {

    private final Multimap<String, Symbol> outputs;
    private final FieldProvider<? extends Symbol> fallback;

    public OutputsAwareFieldProvider(Multimap<String, Symbol> outputs, FieldProvider<? extends Symbol> fallback) {
        this.outputs = outputs;
        this.fallback = fallback;
    }

    @Override
    public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 1) {
            Symbol symbol = RelationAnalyzer.getOneOrAmbiguous(outputs, parts.get(0));
            if (symbol != null) {
                return symbol;
            }
        }
        return fallback.resolveField(qualifiedName, path, operation);
    }
}
