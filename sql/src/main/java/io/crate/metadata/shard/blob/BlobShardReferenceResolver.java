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

package io.crate.metadata.shard.blob;

import io.crate.blob.v2.BlobShard;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.LiteralNestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardMinLuceneVersionExpression;
import io.crate.expression.reference.sys.shard.ShardPathExpression;
import io.crate.expression.reference.sys.shard.ShardPrimaryExpression;
import io.crate.expression.reference.sys.shard.ShardRecoveryExpression;
import io.crate.expression.reference.sys.shard.ShardRelocatingNodeExpression;
import io.crate.expression.reference.sys.shard.ShardRoutingStateExpression;
import io.crate.expression.reference.sys.shard.ShardStateExpression;
import io.crate.expression.reference.sys.shard.blob.BlobShardBlobPathExpression;
import io.crate.expression.reference.sys.shard.blob.BlobShardNumDocsExpression;
import io.crate.expression.reference.sys.shard.blob.BlobShardSizeExpression;
import io.crate.expression.reference.sys.shard.blob.BlobShardTableNameExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MapBackedRefResolver;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.shard.NodeNestableInput;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;

public class BlobShardReferenceResolver {

    public static ReferenceResolver<NestableInput<?>> create(BlobShard blobShard, DiscoveryNode localNode) {
        IndexShard indexShard = blobShard.indexShard();
        ShardId shardId = indexShard.shardId();
        HashMap<ColumnIdent, NestableInput> implementations = new HashMap<>(15);
        implementations.put(SysShardsTableInfo.Columns.ID, new LiteralNestableInput<>(shardId.id()));
        implementations.put(SysShardsTableInfo.Columns.NUM_DOCS, new BlobShardNumDocsExpression(blobShard));
        implementations.put(SysShardsTableInfo.Columns.PRIMARY, new ShardPrimaryExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.RELOCATING_NODE,
            new ShardRelocatingNodeExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.SCHEMA_NAME,
            new LiteralNestableInput<>(new BytesRef(BlobSchemaInfo.NAME)));
        implementations.put(SysShardsTableInfo.Columns.SIZE, new BlobShardSizeExpression(blobShard));
        implementations.put(SysShardsTableInfo.Columns.STATE, new ShardStateExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.ROUTING_STATE, new ShardRoutingStateExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.TABLE_NAME, new BlobShardTableNameExpression(shardId));
        implementations.put(SysShardsTableInfo.Columns.PARTITION_IDENT,
            new LiteralNestableInput<>(new BytesRef("")));
        implementations.put(SysShardsTableInfo.Columns.ORPHAN_PARTITION,
            new LiteralNestableInput<>(false));
        implementations.put(SysShardsTableInfo.Columns.PATH, new ShardPathExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.BLOB_PATH, new BlobShardBlobPathExpression(blobShard));
        implementations.put(
            SysShardsTableInfo.Columns.MIN_LUCENE_VERSION,
            new ShardMinLuceneVersionExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.RECOVERY, new ShardRecoveryExpression(indexShard));
        implementations.put(SysShardsTableInfo.Columns.NODE, new NodeNestableInput(localNode));
        return new MapBackedRefResolver(implementations);
    }

}
