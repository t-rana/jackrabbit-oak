/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.server;

import java.io.File;
import java.net.URI;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Testcase for {@link EmbeddedSolrServerProvider}
 * <p>
 * @deprecated Solr support is deprecated and will be removed in a future version of Oak; see <a href=https://issues.apache.org/jira/browse/OAK-11314 target=_blank>Jira ticket OAK-11314</a> for more information.
 */
@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@Deprecated(forRemoval=true, since="1.74.0")
public class EmbeddedSolrServerProviderTest {

    @Test
    public void testEmbeddedSolrServerInitialization() throws Exception {
        URI uri = getClass().getResource("/solr").toURI();
        File file = new File(uri);
        EmbeddedSolrServerConfiguration solrServerConfiguration = new EmbeddedSolrServerConfiguration(file.getAbsolutePath(), "oak");
        EmbeddedSolrServerProvider embeddedSolrServerProvider = new EmbeddedSolrServerProvider(solrServerConfiguration);
        SolrClient solrServer = embeddedSolrServerProvider.getSolrServer();
        assertNotNull(solrServer);
        try {
            SolrPingResponse ping = solrServer.ping();
            assertNotNull(ping);
            assertEquals(0, ping.getStatus());
        } finally {
            solrServer.close();
        }
    }

}
