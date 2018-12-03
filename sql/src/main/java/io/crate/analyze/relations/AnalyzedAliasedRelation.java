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

import io.crate.analyze.Fields;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import java.util.List;

public final class AnalyzedAliasedRelation implements AnalyzedRelation {

    private final String alias;
    private final AnalyzedRelation childRelation;
    private final Fields fields;

    public AnalyzedAliasedRelation(String alias, AnalyzedRelation childRelation) {
        this.alias = alias;
        this.childRelation = childRelation;
        this.fields = new Fields(childRelation.fields().size());
        for (Field field : childRelation.fields()) {
            this.fields.add(field.path(), new Field(this, field.path(), field.valueType()));
        }
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitAliasRelation(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation == Operation.READ) {
            return fields.get(path);
        }
        throw new UnsupportedOperationException("Fields on views are read-only");
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return null;
    }

    public AnalyzedRelation childRelation() {
        return childRelation;
    }

    public String alias() {
        return alias;
    }

    @Override
    public String toString() {
        return childRelation.toString() + " AS " + alias;
    }
}
