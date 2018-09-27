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

package io.crate.metadata.pgcatalog;

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

public class PgIndexTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_index");

    static class Columns {
        static final ColumnIdent INDRELID = new ColumnIdent("indrelid");
        static final ColumnIdent INDEXRELID = new ColumnIdent("indexrelid");
        static final ColumnIdent INDISPRIMARY = new ColumnIdent("indisprimary");
        static final ColumnIdent INDKEY = new ColumnIdent("indkey");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<RelationInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<RelationInfo>>builder()
            .put(Columns.INDRELID, () -> NestableCollectExpression.constant(0)) // -> pg_class for the table entry
            .put(Columns.INDEXRELID, () -> NestableCollectExpression.constant(0)) // -> pg_class for this index entry
            .put(Columns.INDISPRIMARY, () -> NestableCollectExpression.constant(false)) // don't support dropping columns
            .put(Columns.INDKEY, () -> NestableCollectExpression.constant(new Double[]{}))
            .build();
    }

    PgIndexTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.INDRELID.name(), DataTypes.INTEGER, null)
                .register(Columns.INDEXRELID.name(), DataTypes.INTEGER, null)
                .register(Columns.INDISPRIMARY.name(), DataTypes.BOOLEAN, null)
                .register(Columns.INDKEY.name(), DataTypes.DOUBLE_ARRAY, null),
            Collections.emptyList());
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, state.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }
}
