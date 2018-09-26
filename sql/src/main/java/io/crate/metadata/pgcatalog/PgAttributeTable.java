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
import io.crate.expression.reference.information.ColumnContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Collections;
import java.util.Map;

public class PgAttributeTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_attribute");

    static class Columns {
        static final ColumnIdent ATTRELID = new ColumnIdent("attrelid");
        static final ColumnIdent ATTHASDEF = new ColumnIdent("atthasdef");
        static final ColumnIdent ATTISDROPPED = new ColumnIdent("attisdropped");
        static final ColumnIdent ATTLEN = new ColumnIdent("attlen");
        static final ColumnIdent ATTNAME = new ColumnIdent("attname");
        static final ColumnIdent ATTNOTNULL = new ColumnIdent("attnotnull");
        static final ColumnIdent ATTNUM = new ColumnIdent("attnum");
        static final ColumnIdent ATTTYPID = new ColumnIdent("atttypid");
        static final ColumnIdent ATTTYPMOD = new ColumnIdent("atttypmod");
    }


    public static Map<ColumnIdent, RowCollectExpressionFactory<ColumnContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ColumnContext>>builder()
            .put(Columns.ATTRELID, () -> NestableCollectExpression.constant(0)) // point to table this column belongs to - pg_class
            .put(Columns.ATTHASDEF, () -> NestableCollectExpression.constant(false)) // don't support default values
            .put(Columns.ATTISDROPPED, () -> NestableCollectExpression.constant(false)) // don't support dropping columns
            .put(Columns.ATTLEN, () -> NestableCollectExpression.forFunction(c -> PGTypes.get(c.info.valueType()).typeLen()))
            .put(Columns.ATTNAME, () -> NestableCollectExpression.forFunction(c -> BytesRefs.toBytesRef(c.info.column().fqn())))
            .put(Columns.ATTNOTNULL, () -> NestableCollectExpression.forFunction(c -> !c.info.isNullable()))
            .put(Columns.ATTNUM, () -> NestableCollectExpression.forFunction(c -> c.ordinal))
            .put(Columns.ATTTYPID, () -> NestableCollectExpression.forFunction(c -> PGTypes.get(c.info.valueType()).oid())) // -> pg_type
            .put(Columns.ATTTYPMOD, () -> NestableCollectExpression.constant(-1))
            .build();
    }

    PgAttributeTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.ATTRELID.name(), DataTypes.INTEGER, null)
                .register(Columns.ATTHASDEF.name(), DataTypes.BOOLEAN, null)
                .register(Columns.ATTISDROPPED.name(), DataTypes.BOOLEAN, null)
                .register(Columns.ATTLEN.name(), DataTypes.INTEGER, null)
                .register(Columns.ATTNAME.name(), DataTypes.STRING, null)
                .register(Columns.ATTNOTNULL.name(), DataTypes.BOOLEAN, null)
                .register(Columns.ATTNUM.name(), DataTypes.SHORT, null)
                .register(Columns.ATTTYPID.name(), DataTypes.INTEGER, null)
                .register(Columns.ATTTYPMOD.name(), DataTypes.INTEGER, null),
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
