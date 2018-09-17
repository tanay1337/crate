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
import io.crate.analyze.expressions.Subscripts;
import io.crate.collections.Lists2;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;

/**
 * FieldProvider that is aware of the symbols in the selectList.
 *
 * This can resolve symbols by aliases. For example:
 *
 * <pre>
 * {@code
 *  select load as l from sys.nodes order by l['1']
 *
 *  selectList:
 *      l --> load
 *
 *  orderBy l['1'] resolves to Reference load.1
 * }
 * </pre>
 *
 */
public final class OutputsAwareFieldProvider implements FieldProvider<Symbol> {

    private final Multimap<String, Symbol> selectList;
    private final FieldProvider<? extends Symbol> fallback;

    OutputsAwareFieldProvider(Multimap<String, Symbol> selectList, FieldProvider<? extends Symbol> fallback) {
        this.selectList = selectList;
        this.fallback = fallback;
    }

    @Override
    public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 1) {
            Symbol fieldReferencedByAlias = RelationAnalyzer.getOneOrAmbiguous(selectList, parts.get(0));
            if (fieldReferencedByAlias != null) {
                if (fieldReferencedByAlias instanceof Field && ((Field) fieldReferencedByAlias).path() instanceof ColumnIdent && (path != null && !path.isEmpty())) {
                    ColumnIdent column = (ColumnIdent) ((Field) fieldReferencedByAlias).path();
                    List<String> fullPath = Lists2.concat(column.path(), path);
                    // Re-create QualifiedName by using the real field name; not the alias
                    Symbol optimizedAccess = fallback.resolveField(new QualifiedName(column.name()), fullPath, operation);
                    if (optimizedAccess != null) {
                        return optimizedAccess;
                    }
                }
                return Subscripts.create(fieldReferencedByAlias, path);
            }
        }
        return fallback.resolveField(qualifiedName, path, operation);
    }
}
