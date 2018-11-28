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

import io.crate.execution.ddl.index.DropIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DropIndexClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<DropIndexRequest> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MetaDataDeleteIndexService deleteIndexService;

    public DropIndexClusterStateTaskExecutor(IndexNameExpressionResolver indexNameExpressionResolver,
                                             MetaDataDeleteIndexService deleteIndexService) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.deleteIndexService = deleteIndexService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, DropIndexRequest request) throws Exception {
        final Set<Index> concreteIndices = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndices(
            currentState, IndicesOptions.lenientExpandOpen(), request.indexName())));
        return deleteIndexService.deleteIndices(currentState, concreteIndices);
    }
}
