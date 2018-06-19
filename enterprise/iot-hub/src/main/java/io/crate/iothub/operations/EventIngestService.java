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

package io.crate.iothub.operations;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.auth.user.User;
import io.crate.auth.user.UserLookup;
import io.crate.exceptions.Exceptions;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EventIngestService {

    private static final Logger LOGGER = Loggers.getLogger(EventIngestService.class);
    private static final Map<QualifiedName, Integer> EVENT_HUB_FIELDS_ORDER = new ImmutableMap.Builder<QualifiedName, Integer>()
        .put(new QualifiedName("partition_id"), 0)
        .put(new QualifiedName("offset"), 1)
        .put(new QualifiedName("sequence_number"), 2)
        .put(new QualifiedName("ts"), 3)
        .put(new QualifiedName("payload"), 4)
        .build();
    private static final List<DataType> FIELD_TYPES = Arrays.asList(
        DataTypes.STRING,
        DataTypes.STRING,
        DataTypes.LONG,
        DataTypes.STRING,
        DataTypes.STRING);

    private final ExpressionAnalyzer expressionAnalyzer;
    private final InputFactory inputFactory;
    private final SQLOperations sqlOperations;
    private final IngestionService ingestionService;
    private final User crateUser;
    private final ExpressionAnalysisContext expressionAnalysisContext;

    public EventIngestService(Functions functions,
                              SQLOperations sqlOperations,
                              UserLookup userLookup,
                              IngestionService ingestionService) {
        this.sqlOperations = sqlOperations;
        this.inputFactory = new InputFactory(functions);
        this.expressionAnalysisContext = new ExpressionAnalysisContext();
        FieldProvider<Symbol> eventFieldsProvider = (qualifiedName, path, operation) -> new InputColumn(EVENT_HUB_FIELDS_ORDER.get(qualifiedName));
        this.expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            TransactionContext.systemTransactionContext(),
            null,
            eventFieldsProvider,
            null);
        this.ingestionService = ingestionService;
        this.crateUser = userLookup.findUser("crate");
    }

    @Nullable
    private static String payloadToString(byte[] payload) {
        try {
            return new String(payload, "UTF8");
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Unsupported Encoding!!! Oh no.");
            return null;
        }
    }

    public void doInsert(PartitionContext context, EventData data, String tableName) {
        String payload = payloadToString(data.getBytes());
        if (payload == null) {
            return;
        }

        Object[] args = new Object[]{
            context.getPartitionId(),
            data.getSystemProperties().getOffset(),
            data.getSystemProperties().getSequenceNumber(),
            payload};
        List<Object> argsAsList = Arrays.asList(args);
        Session session = sqlOperations.createSession(Schemas.DOC_SCHEMA_NAME, crateUser, Option.NONE, 1);
        LOGGER.info("Payload: " + payload);
        try {
            session.parse(Session.UNNAMED, "insert into " + RelationName.fromIndexName(tableName).fqn() +
                                           " (\"partition_id\", \"offset\", \"sequence_number\", \"ts\", \"payload\") " +
                                           "values (?, ?, ?, CURRENT_TIMESTAMP, ?)", FIELD_TYPES);
            session.bind(Session.UNNAMED, Session.UNNAMED, argsAsList, null);
            BaseResultReceiver resultReceiver = new BaseResultReceiver();
            resultReceiver.completionFuture().exceptionally(t -> {
                Exceptions.rethrowUnchecked(t);
                return null;
            });
            session.execute(Session.UNNAMED, 0, resultReceiver);
            session.sync();
        } catch (SQLActionException e) {
            LOGGER.error(e.toString());
        }
        session.close();
    }
}
