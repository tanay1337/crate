package org.cratedb.test.integration;


import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.ElasticsearchTestCase;
import org.elasticsearch.TestCluster;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.hamcrest.Matchers;
import org.junit.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * mostly taken from org.elasticsearch.AbstractSharedClusterTest
 */
@AbstractRandomizedTest.IntegrationTests
public abstract class AbstractSharedCrateClusterTest extends ElasticsearchTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static CrateTestCluster cluster;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cluster();
    }

    @Before
    public final void before() throws Exception {
        cluster.ensureAtLeastNumNodes(numberOfNodes());
        wipeIndices();
        wipeTemplates();
        setUp();
    }

    @After
    public void after() throws IOException {
        logger.info("Cleaning up after test.");
        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                .persistentSettings().getAsMap().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                .persistentSettings().getAsMap().size(), equalTo(0));
        wipeIndices(); // wipe after to make sure we fail in the test that didn't ack the delete
        wipeTemplates();
        ensureAllFilesClosed();
    }

    public static TestCluster cluster() {
        if (cluster == null) {
            cluster = new CrateTestCluster(getRandom());
        }
        return cluster;
    }

    public ClusterService clusterService() {
        return cluster().clusterService();
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
            cluster.close();
        }
        cluster = null;
    }

    public static Client client() {
        return cluster().client();
    }

    public static Iterable<Client> clients() {
        return cluster().clients();
    }

    public static ClusterState clusterState() {
        return client().admin().cluster().prepareState().execute().actionGet().getState();
    }

    public void createBlobIndex(String... names) {
        ImmutableSettings.Builder builder = randomSettingsBuilder();
        builder.put("blobs.enabled", true);
        createIndex(builder.build(), names);
    }

    public void createIndex(Settings settings, String... names) {
        for (String name : names) {
            try {
                assertAcked(prepareCreate(name).setSettings(settings));
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            assertAcked(prepareCreate(name).setSettings(settings));
        }
    }

    public BulkResponse loadBulk(String path, Class<?> aClass) throws Exception {
        byte[] bulkPayload = PathAccessor.bytesFromPath(path, aClass);
        BulkResponse bulk = client().prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
        return bulk;
    }



    public ImmutableSettings.Builder randomSettingsBuilder() {
        // TODO RANDOMIZE
        return ImmutableSettings.builder();
    }

    public Settings getSettings() {
        return randomSettingsBuilder().build();
    }

    public static void wipeIndices(String... names) {
        try {
            assertAcked(client().admin().indices().prepareDelete(names));
        } catch (IndexMissingException e) {
            // ignore
        }
    }

    public static void wipeIndex(String name) {
        wipeIndices(name);
    }

    /**
     * Deletes index templates, support wildcard notation.
     */
    public static void wipeTemplates(String... templates) {
        // if nothing is provided, delete all
        if (templates.length == 0) {
            templates = new String[]{"*"};
        }
        for (String template : templates) {
            try {
                client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
            } catch (IndexTemplateMissingException e) {
                // ignore
            }
        }
    }

    public void createIndex(String... names) {
        for (String name : names) {
            try {
                assertAcked(prepareCreate(name).setSettings(getSettings()));
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            assertAcked(prepareCreate(name).setSettings(getSettings()));
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, ImmutableSettings.Builder builder) {
        cluster().ensureAtLeastNumNodes(numNodes);
        Settings settings = getSettings();
        builder.put(settings);
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private ImmutableSettings.Builder getExcludeSettings(String index, int num, ImmutableSettings.Builder builder) {
        String exclude = Joiner.on(',').join(cluster().allButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    public Set<String> getExcludeNodes(String index, int num) {
        Set<String> nodeExclude = cluster().nodeExclude(index);
        Set<String> nodesInclude = cluster().nodesInclude(index);
        if (nodesInclude.size() < num) {
            Iterator<String> limit = Iterators.limit(nodeExclude.iterator(), num - nodesInclude.size());
            while (limit.hasNext()) {
                limit.next();
                limit.remove();
            }
        } else {
            Iterator<String> limit = Iterators.limit(nodesInclude.iterator(), nodesInclude.size() - num);
            while (limit.hasNext()) {
                nodeExclude.add(limit.next());
                limit.remove();
            }
        }
        return nodeExclude;
    }

    public void allowNodes(String index, int numNodes) {
        cluster().ensureAtLeastNumNodes(numNodes);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index).setSettings(getSettings());
    }

    public void updateClusterSettings(Settings settings) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
    }

    public ClusterHealthStatus ensureGreen() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForRelocatingShards(0);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(request).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus ensureYellow() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        return actionGet.getStatus();
    }

    public static String commaString(Iterable<String> strings) {
        return Joiner.on(',').join(strings);
    }

    protected int numberOfNodes() {
        return 2;
    }

    // utils
    protected IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected GetResponse get(String index, String type, String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    public RefreshResponse refresh() {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected void flushAndRefresh() {
        flush(true);
        refresh();
    }

    protected FlushResponse flush() {
        return flush(true);
    }

    protected FlushResponse flush(boolean ignoreNotAllowed) {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush().execute().actionGet();
        if (ignoreNotAllowed) {
            for (ShardOperationFailedException failure : actionGet.getShardFailures()) {
                if (!failure.reason().contains("FlushNotAllowed")) {
                    assert false : "unexpected failed flush " + failure.reason();
                }
            }
        } else {
            assertNoFailures(actionGet);
        }
        return actionGet;
    }

    protected OptimizeResponse optimize() {
        waitForRelocation();
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<String>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator.asUnordered()) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }

    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    protected AdminClient admin() {
        return client().admin();
    }

    protected <Res extends ActionResponse> Res run(ActionRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        return actionGet;
    }

    protected <Res extends BroadcastOperationResponse> Res run(BroadcastOperationRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    // TODO move this into a base class for integration tests
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        if (builders.length == 0) {
            return;
        }
        Random random = getRandom();
        Set<String> indicesSet = new HashSet<String>();
        for (int i = 0; i < builders.length; i++) {
            indicesSet.add(builders[i].request().index());
        }
        final String[] indices = indicesSet.toArray(new String[0]);
        List<IndexRequestBuilder> list = Arrays.asList(builders);
        Collections.shuffle(list, random);
        final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<Throwable>();
        List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
        if (frequently()) {
            logger.info("Index [{}] docs async: [{}]", list.size(), true);
            final CountDownLatch latch = new CountDownLatch(list.size());
            latches.add(latch);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute(new LatchedActionListener<IndexResponse>(latch, errors));
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<FlushResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIgnoreIndices(IgnoreIndices.MISSING).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches), errors));
                    }
                }
            }

        } else {
            logger.info("Index [{}] docs async: [{}]", list.size(), false);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute().actionGet();
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<FlushResponse>(newLatch(latches), errors));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIgnoreIndices(IgnoreIndices.MISSING).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches), errors));
                    }
                }
            }
        }
        for (CountDownLatch countDownLatch : latches) {
            countDownLatch.await();
        }
        assertThat(errors, Matchers.emptyIterable());
        if (forceRefresh) {
            assertNoFailures(client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute().get());
        }
    }

    private static final CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    private static class LatchedActionListener<Response> implements ActionListener<Response> {
        private final CountDownLatch latch;
        private final CopyOnWriteArrayList<Throwable> errors;

        public LatchedActionListener(CountDownLatch latch, CopyOnWriteArrayList<Throwable> errors) {
            this.latch = latch;
            this.errors = errors;
        }

        @Override
        public void onResponse(Response response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable e) {
            try {
                errors.add(e);
            } finally {
                latch.countDown();
            }
        }

    }

    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }

}
