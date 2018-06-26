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

package io.crate.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.collect.ImmutableList;
import io.crate.testing.SQLResponse;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 3, maxNumDataNodes = 3, transportClientRatio = 0, numClientNodes = 0)
public class GroupByDuringNetworkDisruptionITest extends SQLTransportIntegrationTest {

    private static final Logger LOGGER =Loggers.getLogger(GroupByDuringNetworkDisruptionITest.class);
    private ExecutorService executorService;
    private AtomicBoolean stopThreads;
    private int numThreads = 10;

    @Before
    public void setupExecutor() throws Exception {
        stopThreads = new AtomicBoolean(false);
        executorService = Executors.newFixedThreadPool(numThreads);
    }

    @After
    public void tearDownExecutor() throws Exception {
        stopThreads.set(true);
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    @Repeat (iterations = 100)
    public void testQueriesWhileNetworkDisruption() throws Exception {
        execute("create table t1 (x int) clustered into 3 shards with (number_of_replicas = 0)");
        Object[][] bulkArgs = IntStream.concat(
            IntStream.range(0, 24),
            IntStream.range(2, 36))
            .mapToObj(x -> new Object[] { x })
            .toArray(Object[][]::new);
        execute("insert into t1 (x) values (?)", bulkArgs);
        execute("refresh table t1");

        final List<ActionFuture<SQLResponse>> responses = new ArrayList<>();
        runConcurrentGroupBy(responses);

        LOGGER.info("Waiting for queries to spawn");
        waitForMoreSpawnedQueries(responses, 20);
        LOGGER.info("Adding NetworkDisruption");
        disruptNodes();

        LOGGER.info("Waiting for queries to spawn (after disrupt)");
        waitForMoreSpawnedQueries(responses, 20);
        LOGGER.info("Waiting for results");
        waitOnResults(responses);
        stopThreads.set(true);

        /*
        LOGGER.info("Clearing disruption");
        clearDisruptionScheme();
        LOGGER.info("Waiting for queries to spawn (after disruption clear)");
        waitForMoreSpawnedQueries(responses, 20);
        LOGGER.info("Stopping threads");
        stopThreads.set(true);
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);

        LOGGER.info("Waiting for results");
        waitOnResults(responses);
        */
    }

    private void runConcurrentGroupBy(List<ActionFuture<SQLResponse>> responses) {
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                while (!stopThreads.get()) {
                    ActionFuture<SQLResponse> r = sqlExecutor.execute("select x, count(*) from t1 group by x", null);
                    synchronized (responses) {
                        responses.add(r);
                    }
                }
            });
        }
    }

    private static void waitOnResults(List<ActionFuture<SQLResponse>> responses) throws Exception {
        ImmutableList<ActionFuture<SQLResponse>> futures;
        synchronized (responses) {
            futures = ImmutableList.copyOf(responses);
            responses.clear();
        }
        for (ActionFuture<SQLResponse> future: futures) {
            assertThat(future.get(2, TimeUnit.SECONDS).rowCount(), is(36L));
        }
    }

    private static void waitForMoreSpawnedQueries(List<ActionFuture<SQLResponse>> responses,
                                                  int numQueries) throws Exception {
        int initialSize;
        synchronized (responses) {
            initialSize = responses.size();
        }
        assertBusy(() -> {
            int size;
            synchronized (responses) {
                size = responses.size();
            }
            assertThat(size, Matchers.greaterThanOrEqualTo(initialSize + numQueries));
        });
    }

    private void disruptNodes() {
        String[] nodeNames = internalCluster().getNodeNames();
        String n1 = nodeNames[0];
        String n2 = nodeNames[1];
        String n3 = nodeNames[2];
        setDisruptionScheme(new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Sets.newHashSet(n1, n2), Collections.singleton(n3)),
            new NetworkDisruption.NetworkUnresponsive()
        ));
    }
}
