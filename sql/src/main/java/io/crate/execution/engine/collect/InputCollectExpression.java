/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect;

import io.crate.data.Row;

public class InputCollectExpression implements CollectExpression<Row, Object> {

    private final int position;
    private Row row;

    public InputCollectExpression(int position) {
        this.position = position;
    }

    @Override
    public void setNextRow(Row row) {
        assert row.numColumns() > position : "row smaller than input position " + row.numColumns() + "<=" + position;
        this.row = row;
    }

    @Override
    public boolean hasValue() {
        return row.hasValue(position);
    }

    @Override
    public Object value() {
        return row.get(position);
    }

    @Override
    public long getLong() {
        return row.getLong(position);
    }

    @Override
    public String toString() {
        return "Input{pos=" + position + '}';
    }
}
