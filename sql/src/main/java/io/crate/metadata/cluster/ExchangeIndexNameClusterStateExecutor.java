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

package io.crate.metadata.cluster;

import io.crate.execution.ddl.index.ExchangeIndexNameRequest;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

public class ExchangeIndexNameClusterStateExecutor extends DDLClusterStateTaskExecutor<ExchangeIndexNameRequest> {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    private static final String BACKUP_PREFIX = ".backup";


    private final Logger logger;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public ExchangeIndexNameClusterStateExecutor(Settings settings,
                                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        logger = Loggers.getLogger(getClass(), settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public ClusterState execute(ClusterState currentState, ExchangeIndexNameRequest request) throws Exception {
        final String sourceIndexName = request.sourceIndexName();
        final String targetIndexName = request.targetIndexName();

        final String backupIndexName = BACKUP_PREFIX +
                                       ((sourceIndexName.startsWith(".")) ? "" : ".") +
                                       sourceIndexName;
        logger.info("exchange index names '{}' <-> '{}'", sourceIndexName, targetIndexName);

        ClusterState state = currentState;
        // source -> backup
        state = renameIndex(state, sourceIndexName, backupIndexName);
        // target -> source
        state = renameIndex(state, targetIndexName, sourceIndexName);
        // backup -> target
        state = renameIndex(state, backupIndexName, targetIndexName);
        return state;
    }


    private ClusterState renameIndex(ClusterState currentState,
                                String sourceIndexName,
                                String targetIndexName) {
        MetaData.Builder mdBuilder = null;
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());

        if (logger.isDebugEnabled()) {
            logger.debug("rename index '{}' to '{}'", sourceIndexName, targetIndexName);
        }

        Index[] sourceIndices = indexNameExpressionResolver.concreteIndices(currentState,
            STRICT_INDICES_OPTIONS, sourceIndexName);

        MetaData metaData = currentState.getMetaData();
        mdBuilder = MetaData.builder(metaData);

        renameIndexMetaData(metaData, mdBuilder, blocksBuilder, sourceIndices[0], targetIndexName);

        // The MetaData will always be overridden (and not merged!) when applying it on a cluster state builder.
        // So we must re-build the state with the latest modifications before we pass this state to possible modifiers
        // or return it.
        // Otherwise they would operate on the old MetaData and would just ignore any modifications.
        currentState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();
        return currentState;
    }

    private static void renameIndexMetaData(MetaData metaData,
                                            MetaData.Builder mdBuilder,
                                            ClusterBlocks.Builder blocksBuilder,
                                            Index sourceIndex,
                                            String targetIndexName) {
        IndexMetaData indexMetaData = metaData.getIndexSafe(sourceIndex);
        IndexMetaData targetIndexMetadata = IndexMetaData.builder(indexMetaData)
            .index(targetIndexName).build();
        mdBuilder.remove(sourceIndex.getName());
        mdBuilder.put(targetIndexMetadata, true);
        blocksBuilder.removeIndexBlocks(sourceIndex.getName());
        blocksBuilder.addBlocks(targetIndexMetadata);
    }
}
