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

import io.crate.analyze.expressions.Subscripts;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.collections.Lists2;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;

/**
 * FieldProvider that is aware of the symbols in the selectList.
 *
 * This can resolve symbols by aliases or by ordinal position. For example:
 *
 * <pre>
 * {@code
 *  select load as X from sys.nodes order by X['1']
 *
 *  selectList:
 *      X --> load
 *
 *  orderBy X['1'] resolves to Reference load.1
 * }
 * </pre>
 *
 * <pre>
 * {@code
 *  select name from sys.nodes order by 1
 *
 *  selectList:
 *      name --> name
 *
 *  orderBy 1 resolves to Reference name
 * }
 * </pre>
 *
 */
public final class OutputsAwareFieldProvider implements FieldProvider<Symbol> {

    private final SelectAnalysis selectAnalysis;
    private final BiFunction<String, List<Symbol>, Symbol> allocateFunction;
    private final FieldProvider<? extends Symbol> fallback;

    OutputsAwareFieldProvider(SelectAnalysis selectAnalysis,
                              BiFunction<String, List<Symbol>, Symbol> allocateFunction,
                              FieldProvider<? extends Symbol> fallback) {
        this.selectAnalysis = selectAnalysis;
        this.allocateFunction = allocateFunction;
        this.fallback = fallback;
    }

    @Override
    public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 1) {
            Symbol fieldReferencedByAlias = RelationAnalyzer.getOneOrAmbiguous(
                selectAnalysis.outputMultiMap(), parts.get(0));
            if (fieldReferencedByAlias != null) {
                Symbol optimizedAccess = tryOptimizeAccessByPath(path, operation, fieldReferencedByAlias);
                return optimizedAccess == null
                    ? Subscripts.create(fieldReferencedByAlias, path, allocateFunction)
                    : optimizedAccess;
            }
        }
        Symbol symbol = fallback.resolveField(qualifiedName, path, operation);
        if (symbol.symbolType().isValueSymbol()) {
            Literal longLiteral;
            try {
                longLiteral = Literal.convert(symbol, DataTypes.LONG);
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Cannot use %s in ORDER BY clause", SymbolPrinter.INSTANCE.printUnqualified(symbol)));
            }
            if (longLiteral.value() == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Cannot use %s in ORDER BY clause", SymbolPrinter.INSTANCE.printUnqualified(symbol)));
            }
            return RelationAnalyzer.ordinalOutputReference(selectAnalysis.outputSymbols(), longLiteral, "ORDER BY");
        }
        return symbol;
    }

    @Nullable
    private Symbol tryOptimizeAccessByPath(@Nullable List<String> path, Operation operation, Symbol fieldReferencedByAlias) {
        boolean fieldIsTableColumn = fieldReferencedByAlias instanceof Field
                    && ((Field) fieldReferencedByAlias).path() instanceof ColumnIdent
                    && (path != null && !path.isEmpty());
        if (fieldIsTableColumn) {
            ColumnIdent column = (ColumnIdent) ((Field) fieldReferencedByAlias).path();
            List<String> fullPath = Lists2.concat(column.path(), path);
            // Re-create QualifiedName by using the real field name; not the alias
            Symbol optimizedAccess = fallback.resolveField(new QualifiedName(column.name()), fullPath, operation);
            if (optimizedAccess != null) {
                return optimizedAccess;
            }
        }
        return null;
    }
}
