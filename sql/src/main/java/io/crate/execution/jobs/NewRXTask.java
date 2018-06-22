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

package io.crate.execution.jobs;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import io.crate.Streamer;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.NewBatchPagingIterator;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class NewRXTask extends AbstractTask implements DownstreamRXTask {

    private final AtomicReference<PageBucketReceiver> currentAction;
    private final WaitForInitialPage waitForInitialPage;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final RowConsumer rowConsumer;

    public NewRXTask(int id,
                     Logger logger,
                     int numBuckets,
                     PagingIterator<Integer, Row> pagingIterator,
                     RowConsumer rowConsumer) {
        super(id, logger);
        this.pagingIterator = pagingIterator;
        this.rowConsumer = rowConsumer;
        waitForInitialPage = new WaitForInitialPage(numBuckets, this::initiateConsumer);
        currentAction = new AtomicReference<>(waitForInitialPage);
    }

    private void initiateConsumer(List<KeyIterable<Integer, Row>> page, IntObjectMap<PageResultListener> listenersByBucketIdx) {
        NewBatchPagingIterator<Integer, Row> iterator = new NewBatchPagingIterator<>(pagingIterator);
        ConsumerActive consumerActive = new ConsumerActive(iterator);
        if (currentAction.compareAndSet(waitForInitialPage, consumerActive)) {
            pagingIterator.merge(page);
            rowConsumer.accept(iterator, null);
        } else {
            throw new IllegalStateException("Can't change from action " + currentAction.get() + " to row consumption");
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        return currentAction.get();
    }


    static class ConsumerActive implements PageBucketReceiver {

        private final BatchIterator<Row> batchIterator;

        ConsumerActive(BatchIterator<Row> batchIterator) {
            this.batchIterator = batchIterator;
        }

        @Override
        public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
            throw new IllegalStateException("Cannot receive more buckets while consumer is active");
        }

        @Override
        public void failure(int bucketIdx, Throwable throwable) {
            throw new IllegalStateException("Cannot receive more buckets while consumer is active");
        }

        @Override
        public void killed(int bucketIdx, Throwable throwable) {
            batchIterator.kill(throwable);
        }

        @Override
        public Streamer<?>[] streamers() {
            return new Streamer[0];
        }
    }

    static class WaitForInitialPage implements PageBucketReceiver {

        private final ArrayList<KeyIterable<Integer, Row>> buckets;
        private final IntObjectHashMap<PageResultListener> listenersByBucketIdx;
        private final BiConsumer<List<KeyIterable<Integer, Row>>, IntObjectMap<PageResultListener>> onPageReceived;
        private final AtomicInteger remainingBucketsToReceive;
        private final BitSet gotLast = new BitSet(0);

        WaitForInitialPage(int numBuckets,
                           BiConsumer<List<KeyIterable<Integer, Row>>, IntObjectMap<PageResultListener>> onPageReceived) {
            this.remainingBucketsToReceive = new AtomicInteger(numBuckets);
            this.buckets = new ArrayList<>(numBuckets);
            this.listenersByBucketIdx = new IntObjectHashMap<>(numBuckets);
            this.onPageReceived = onPageReceived;
        }

        @Override
        public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
            KeyIterable<Integer, Row> bucket = new KeyIterable<>(bucketIdx, rows);
            synchronized (buckets) {
                buckets.add(bucket);
                gotLast.set(bucketIdx);
                listenersByBucketIdx.put(bucketIdx, pageResultListener);
            }
            int remaining = remainingBucketsToReceive.decrementAndGet();
            if (remaining == 0) {
                onPageReceived.accept(buckets, listenersByBucketIdx);
            }
        }

        @Override
        public void failure(int bucketIdx, Throwable throwable) {
        }

        @Override
        public void killed(int bucketIdx, Throwable throwable) {
        }

        @Override
        public Streamer<?>[] streamers() {
            return new Streamer[0];
        }
    }
}
