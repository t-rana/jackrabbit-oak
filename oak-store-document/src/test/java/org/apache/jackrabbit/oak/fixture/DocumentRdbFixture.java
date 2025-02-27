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

package org.apache.jackrabbit.oak.fixture;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RdbConnectionUtils;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;

public class DocumentRdbFixture extends NodeStoreFixture {

    private final Map<NodeStore, DataSource> dataSources = new ConcurrentHashMap<NodeStore, DataSource>();

    private static final String jdbcUrl = RdbConnectionUtils.mapJdbcURL();

    @Override
    public NodeStore createNodeStore() {
        String prefix = "T" + Long.toHexString(System.currentTimeMillis());
        RDBOptions options = new RDBOptions().tablePrefix(prefix).dropTablesOnClose(true);
        DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcUrl, RdbConnectionUtils.USERNAME, RdbConnectionUtils.PASSWD);
        //do not reuse the whiteboard
        setWhiteboard(new DefaultWhiteboard());
        RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
        builder.setNoChildOrderCleanupFeature(Feature.newFeature("FT_NOCOCLEANUP_OAK-10660", getWhiteboard()));
        NodeStore result = builder.setPersistentCache("target/persistentCache,time")
                .setRDBConnection(ds, options).build();
        this.dataSources.put(result, ds);
        return result;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        if (nodeStore instanceof DocumentNodeStore) {
            ((DocumentNodeStore) nodeStore).dispose();
        }
        DataSource ds = this.dataSources.remove(nodeStore);
        if (ds instanceof Closeable) {
            try {
                ((Closeable)ds).close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public String toString() {
        return "DocumentNodeStore[RDB] on " + Objects.toString(this.jdbcUrl);
    }
}