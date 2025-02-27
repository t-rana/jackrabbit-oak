/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.junit.After;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.JOURNAL_CREATE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.JOURNAL_CREATE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_CREATE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_CREATE_SPLIT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_CREATE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_CREATE_UPSERT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_CREATE_UPSERT_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_UPDATE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_UPDATE_FAILURE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_UPDATE_RETRY_COUNT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreStats.NODES_UPDATE_TIMER;
import static org.junit.Assert.assertEquals;

public class DocumentStoreStatsTest {
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(),executor);
    private DocumentStoreStats stats = new DocumentStoreStats(statsProvider);

    @After
    public void shutDown(){
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void doneFindCached() throws Exception{
        stats.doneFindCached(Collection.NODES, "foo");
        assertEquals(1, getMeter(DocumentStoreStats.NODES_FIND_CACHED).getCount());

        stats.doneFindCached(Collection.SETTINGS, "foo");
        assertEquals(1, getMeter(DocumentStoreStats.NODES_FIND_CACHED).getCount());
    }

    @Test
    public void doneFindUncached() throws Exception{
        stats.doneFindUncached(100, Collection.NODES, "0:/", true, false);
        assertEquals(1, getMeter(DocumentStoreStats.NODES_FIND_PRIMARY).getCount());
        assertEquals(100, getTimer(DocumentStoreStats.NODES_FIND_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(DocumentStoreStats.NODES_FIND_SLAVE).getCount());

        stats.doneFindUncached(100, Collection.NODES, "0:/", true, true);
        assertEquals(1, getMeter(DocumentStoreStats.NODES_FIND_PRIMARY).getCount());
        assertEquals(1, getMeter(DocumentStoreStats.NODES_FIND_SLAVE).getCount());

        stats.doneFindUncached(100, Collection.NODES, "2:p/foo", true, false);
        assertEquals(2, getMeter(DocumentStoreStats.NODES_FIND_PRIMARY).getCount());
        assertEquals(1, getMeter(DocumentStoreStats.NODES_FIND_SPLIT).getCount());

        stats.doneFindUncached(100, Collection.SETTINGS, "0:/", true, false);
        //Nothing change for other collection
        assertEquals(2, getMeter(DocumentStoreStats.NODES_FIND_PRIMARY).getCount());
    }

    @Test
    public void doneQuery_Nodes() throws Exception{
        stats.doneQuery(100, Collection.NODES, "foo", "bar", false, 5, -1, false);
        assertEquals(5, getMeter(DocumentStoreStats.NODES_QUERY_FIND_READ_COUNT).getCount());
        assertEquals(1, getMeter(DocumentStoreStats.NODES_QUERY_PRIMARY).getCount());

        stats.doneQuery(100, Collection.NODES, "foo", "bar", false, 7, -1, true);
        assertEquals(1, getMeter(DocumentStoreStats.NODES_QUERY_SLAVE).getCount());
        assertEquals(12, getMeter(DocumentStoreStats.NODES_QUERY_FIND_READ_COUNT).getCount());

        stats.doneQuery(100, Collection.NODES, "foo", "bar", false, 7, 1000, false);
        assertEquals(2, getMeter(DocumentStoreStats.NODES_QUERY_PRIMARY).getCount());
        assertEquals(1, getMeter(DocumentStoreStats.NODES_QUERY_LOCK).getCount());
    }

    @Test
    public void doneQuery_Journal() throws Exception{
        stats.doneQuery(100, Collection.JOURNAL, "foo", "bar", false, 5, -1, false);
        assertEquals(5, getMeter(DocumentStoreStats.JOURNAL_QUERY).getCount());
        assertEquals(1, getTimer(DocumentStoreStats.JOURNAL_QUERY_TIMER).getCount());
    }

    @Test
    public void doneCreate_Journal() throws Exception{
        stats.doneCreate(100, Collection.JOURNAL, List.of("a", "b"), true);
        assertEquals(2, getMeter(DocumentStoreStats.JOURNAL_CREATE).getCount());
        assertEquals(100, getTimer(DocumentStoreStats.JOURNAL_CREATE_TIMER).getSnapshot().getMax());

        stats.doneCreate(100, JOURNAL, List.of("c", "d"), false);
        assertEquals(4, getMeter(JOURNAL_CREATE).getCount());
        assertEquals(100, getTimer(JOURNAL_CREATE_TIMER).getSnapshot().getMax());
    }

    @Test
    public void doneCreate_Nodes() {

        // empty list of ids
        stats.doneCreate(100, NODES, List.of(), true);
        assertEquals(0, getMeter(NODES_CREATE).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(0, getTimer(NODES_CREATE_TIMER).getSnapshot().getMax());

        stats.doneCreate(100, NODES, List.of("a", "b"), true);
        assertEquals(2, getMeter(NODES_CREATE).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(50, getTimer(NODES_CREATE_TIMER).getSnapshot().getMax());

        // adding an Id with previous doc
        stats.doneCreate(200, NODES, List.of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"), true);
        assertEquals(3, getMeter(NODES_CREATE).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(200, getTimer(NODES_CREATE_TIMER).getSnapshot().getMax());

        // if insert is not successful
        stats.doneCreate(200, NODES, List.of("c"), false);
        assertEquals(3, getMeter(NODES_CREATE).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(200, getTimer(NODES_CREATE_TIMER).getSnapshot().getMax());

        // journal metrics are not updated
        assertEquals(0, getMeter(JOURNAL_CREATE).getCount());
        assertEquals(0, getTimer(JOURNAL_CREATE_TIMER).getSnapshot().getMax());
    }

    @Test
    public void doneCreateOrUpdate() {

        // empty list of ids
        stats.doneCreateOrUpdate(100, NODES, List.of());
        assertEquals(0, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());

        stats.doneCreateOrUpdate(100, NODES, List.of("a", "b"));
        assertEquals(2, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(50, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());

        // adding an Id with previous Doc
        stats.doneCreateOrUpdate(200, NODES, List.of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"));
        assertEquals(3, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(200, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());

        // insert is done for journal collection
        stats.doneCreateOrUpdate(200, JOURNAL, List.of("c"));
        assertEquals(3, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(200, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());

    }

    @Test
    public void doneFindAndModify() throws Exception{
        stats.doneFindAndModify(100, Collection.NODES, "foo", true, true, 0);
        assertEquals(1, getMeter(DocumentStoreStats.NODES_CREATE_UPSERT).getCount());
        assertEquals(100, getTimer(DocumentStoreStats.NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());

        stats.doneFindAndModify(100, Collection.NODES, "foo", false, true, 0);
        assertEquals(1, getMeter(NODES_UPDATE).getCount());
        assertEquals(100, getTimer(NODES_UPDATE_TIMER).getSnapshot().getMax());
    }

    @Test
    public void doneBulkFindAndModify() {

        // no node had been updated i.e. empty list of Ids
        stats.doneFindAndModify(100, NODES, List.of(), true, 0);
        assertEquals(0, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(NODES_UPDATE).getCount());
        assertEquals(0, getTimer(NODES_UPDATE_TIMER).getSnapshot().getMax());

        // 2 nodes had been updated
        stats.doneFindAndModify(100, NODES, List.of("foo", "bar"), true, 0);
        assertEquals(0, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());
        assertEquals(2, getMeter(NODES_UPDATE).getCount());
        assertEquals(50, getTimer(NODES_UPDATE_TIMER).getSnapshot().getMax());

        // fails to update 2 nodes without retrying
        stats.doneFindAndModify(100, NODES, List.of("foo", "bar"), false, 0);
        assertEquals(0, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());
        assertEquals(2, getMeter(NODES_UPDATE).getCount());
        assertEquals(50, getTimer(NODES_UPDATE_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
        assertEquals(1, getMeter(NODES_UPDATE_FAILURE).getCount());

        // update is done on Journal collection
        stats.doneFindAndModify(100, JOURNAL, List.of("foo", "bar"), true, 0);
        assertEquals(0, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());
        assertEquals(2, getMeter(NODES_UPDATE).getCount());
        assertEquals(50, getTimer(NODES_UPDATE_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
        assertEquals(1, getMeter(NODES_UPDATE_FAILURE).getCount());

    }

    @Test
    public void doneFindAndModifyRetryAndFailure() throws Exception{
        stats.doneFindAndModify(100, Collection.NODES, "foo", true, false, 3);
        assertEquals(1, getMeter(DocumentStoreStats.NODES_UPDATE_FAILURE).getCount());
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());

        stats.doneFindAndModify(100, Collection.NODES, "foo", true, true, 2);
        assertEquals(5, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
    }

    @Test
    public void doneRemove() throws Exception {
        stats.doneRemove(100, Collection.NODES, 42);
        assertEquals(42, getMeter(DocumentStoreStats.NODES_REMOVE).getCount());

        stats.doneRemove(100, Collection.NODES, 17);
        assertEquals(59, getMeter(DocumentStoreStats.NODES_REMOVE).getCount());
    }

    @Test
    public void perfLog() throws Exception{
        String logName = DocumentStoreStats.class.getName() + ".perf";
        LogCustomizer customLogs = LogCustomizer.forLogger(logName)
                .filter(Level.TRACE)
                .create();

        enableLevel(logName, Level.INFO);
        customLogs.starting();

        //No logs untill debug enabled
        stats.doneFindAndModify(100, Collection.NODES, "foo", true, true, 0);
        assertEquals(0, customLogs.getLogs().size());

        stats.doneFindAndModify(TimeUnit.SECONDS.toNanos(10), Collection.NODES, "foo", true, true, 0);
        assertEquals(0, customLogs.getLogs().size());

        //Change level to DEBUG - Now threshold rule applies
        enableLevel(logName, Level.DEBUG);

        stats.doneFindAndModify(100, Collection.NODES, "foo", true, true, 0);
        assertEquals(0, customLogs.getLogs().size());

        stats.doneFindAndModify(TimeUnit.SECONDS.toNanos(10), Collection.NODES, "foo", true, true, 0);
        assertEquals(1, customLogs.getLogs().size());

        //With trace level everything is logged
        enableLevel(logName, Level.TRACE);
        stats.doneFindAndModify(100, Collection.NODES, "foo", true, true, 0);
        assertEquals(2, customLogs.getLogs().size());

        customLogs.finished();
    }


    private Meter getMeter(String name) {
        return statsProvider.getRegistry().getMeters().get(name);
    }

    private Timer getTimer(String name) {
        return statsProvider.getRegistry().getTimers().get(name);
    }

    private static void enableLevel(String logName, Level level){
        ((LoggerContext)LoggerFactory.getILoggerFactory())
                .getLogger(logName).setLevel(level);
    }
}