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

package io.crate.analyze.expressions;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.SubscriptContext;
import io.crate.analyze.SubscriptValidator;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.SubscriptObjectFunction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Used to convert subscript expressions into symbols.
 */
public final class Subscripts {

    /**
     * <pre>
     * {@code
     *  input: tags[1]['name']
     *      name:   subscript{name=tags, index=1}
     *      index:  name
     *
     *  output: tags.name[1]
     *
     * }
     * </pre>
     */
    public static Symbol convert(SubscriptExpression subscript,
                                 FieldProvider<? extends Symbol> fieldProvider,
                                 Operation operation,
                                 Function<? super Expression, ? extends Symbol> expression2Symbol,
                                 BiFunction<String, List<Symbol>, Symbol> allocateFunction) {
        SubscriptContext subscriptContext = new SubscriptContext();
        SubscriptValidator.validate(subscript, subscriptContext);
        QualifiedName qualifiedName = subscriptContext.qualifiedName();
        List<String> parts = subscriptContext.parts();

        if (qualifiedName == null) {
            if (parts == null || parts.isEmpty()) {
                Symbol name = expression2Symbol.apply(subscript.name());
                Symbol index = expression2Symbol.apply(subscript.index());
                return createSubscript(allocateFunction, name, index);
            } else {
                Symbol name = expression2Symbol.apply(subscriptContext.expression());
                return create(name, parts, allocateFunction);
            }
        } else {
            Symbol symbol = fieldProvider.resolveField(qualifiedName, parts, operation);
            Expression idxExpression = subscriptContext.index();
            if (idxExpression == null) {
                return symbol;
            }
            Symbol index = expression2Symbol.apply(idxExpression);
            return createSubscript(allocateFunction, symbol, index);
        }
    }

    private static Symbol createSubscript(BiFunction<String, List<Symbol>, Symbol> allocateFunction,
                                          Symbol name,
                                          Symbol index) {
        String function = name.valueType().equals(DataTypes.OBJECT)
            ? SubscriptObjectFunction.NAME
            : SubscriptFunction.NAME;
        return allocateFunction.apply(function, ImmutableList.of(name, index));
    }

    public static Symbol create(Symbol symbol,
                                @Nullable List<String> path,
                                BiFunction<String, List<Symbol>, Symbol> allocateFunction) {
        if (path == null || path.isEmpty()) {
            return symbol;
        }
        for (int i = 0; i < path.size(); i++) {
            symbol = allocateFunction.apply(
                SubscriptObjectFunction.NAME,
                ImmutableList.of(symbol, Literal.of(path.get(i)))
            );
        }
        return symbol;
    }
}
