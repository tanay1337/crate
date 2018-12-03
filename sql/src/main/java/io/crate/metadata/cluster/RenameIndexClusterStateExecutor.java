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

import io.crate.execution.ddl.index.BulkRenameIndexRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;

public class RenameIndexClusterStateExecutor extends DDLClusterStateTaskExecutor<BulkRenameIndexRequest> {

    private final Logger logger;

    public RenameIndexClusterStateExecutor() {
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public ClusterState execute(ClusterState currentState, BulkRenameIndexRequest request) throws Exception {

        final String[] sourceIndexNames = request.sourceIndices();
        final String[] targetIndexNames = request.targetIndices();

        logger.info("rename indexes '[{}]' to '[{}]'", sourceIndexNames, targetIndexNames);
        MetaData.Builder mdBuilder = null;
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());

        MetaData metaData = currentState.getMetaData();
        mdBuilder = MetaData.builder(metaData);

        renameIndices(metaData, mdBuilder, blocksBuilder, sourceIndexNames, targetIndexNames);
        return ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();
    }

    static void renameIndices(MetaData metaData,
                              MetaData.Builder mdBuilder,
                              ClusterBlocks.Builder blocksBuilder,
                              String[] sourceIndexNames,
                              String[] targetIndexNames) {
        for (int i = 0; i < sourceIndexNames.length; i++) {
            String sourceIndex = sourceIndexNames[i];
            IndexMetaData indexMetaData = metaData.index(sourceIndex);
            IndexMetaData targetIndexMetadata = IndexMetaData.builder(indexMetaData)
                .index(targetIndexNames[i]).build();
            mdBuilder.remove(sourceIndex);
            mdBuilder.put(targetIndexMetadata, true);
            blocksBuilder.removeIndexBlocks(sourceIndex);
            blocksBuilder.addBlocks(targetIndexMetadata);
        }
    }
}
