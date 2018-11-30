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

import io.crate.execution.ddl.index.RenameIndexRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import static io.crate.metadata.cluster.DDLClusterStateHelpers.renameIndices;

public class RenameIndexClusterStateExecutor extends DDLClusterStateTaskExecutor<RenameIndexRequest> {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);


    private final Logger logger;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public RenameIndexClusterStateExecutor(Settings settings,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        logger = LogManager.getLogger(getClass());

        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public ClusterState execute(ClusterState currentState, RenameIndexRequest request) throws Exception {
        final String[] sourceIndexName = request.sourceIndexName();
        final String[] targetIndexName = request.targetIndexName();

        logger.info("rename indexes '[{}]' to '[{}]'", sourceIndexName, targetIndexName);

        MetaData.Builder mdBuilder = null;
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());

        Index[] sourceIndices = indexNameExpressionResolver.concreteIndices(currentState,
            STRICT_INDICES_OPTIONS, sourceIndexName);
        Index[] sourceIndicesOrdered = new Index[sourceIndexName.length];

        // ensure the order in input is respected
        for (int i = 0; i < sourceIndexName.length; i++) {
            for (int j = 0; j < sourceIndices.length; j++) {
                if (sourceIndexName[i].equals(sourceIndices[j].getName())) {
                    sourceIndicesOrdered[i] = sourceIndices[j];
                    break;
                }
            }
        }

        MetaData metaData = currentState.getMetaData();
        mdBuilder = MetaData.builder(metaData);

        renameIndices(metaData, mdBuilder, blocksBuilder, sourceIndicesOrdered, targetIndexName);

        // The MetaData will always be overridden (and not merged!) when applying it on a cluster state builder.
        // So we must re-build the state with the latest modifications before we pass this state to possible modifiers
        // or return it.
        // Otherwise they would operate on the old MetaData and would just ignore any modifications.
        currentState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();
        return currentState;
    }
}
