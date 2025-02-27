/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.plugins.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;
import org.slf4j.event.Level;

public class ReadWriteNamespaceRegistryTest extends OakBaseTest {

    public ReadWriteNamespaceRegistryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Override
    protected ContentSession createContentSession() {
        return new Oak(store).with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new NamespaceEditorProvider()).createContentSession();
    }

    @Test
    public void testMappings() throws Exception {
        final ContentSession session = createContentSession();
        final Root root = session.getLatestRoot();
        NamespaceRegistry r = getNamespaceRegistry(session, root);

        assertEquals("", r.getURI(""));
        assertEquals("http://www.jcp.org/jcr/1.0", r.getURI("jcr"));
        assertEquals("http://www.jcp.org/jcr/nt/1.0", r.getURI("nt"));
        assertEquals("http://www.jcp.org/jcr/mix/1.0", r.getURI("mix"));
        assertEquals("http://www.w3.org/XML/1998/namespace", r.getURI("xml"));

        assertEquals("", r.getPrefix(""));
        assertEquals("jcr", r.getPrefix("http://www.jcp.org/jcr/1.0"));
        assertEquals("nt", r.getPrefix("http://www.jcp.org/jcr/nt/1.0"));
        assertEquals("mix", r.getPrefix("http://www.jcp.org/jcr/mix/1.0"));
        assertEquals("xml", r.getPrefix("http://www.w3.org/XML/1998/namespace"));

        r.registerNamespace("p", "n");
        assertEquals(r.getURI("p"), "n");
        assertEquals(r.getPrefix("n"), "p");

        r.registerNamespace("p2", "n2");
        assertEquals(r.getURI("p2"), "n2");
        assertEquals(r.getPrefix("n2"), "p2");

        // xml namespace check
        assertTrue(SetUtils.toSet(r.getPrefixes()).contains("xml"));
        try {
            r.registerNamespace("xml", "test");
            fail("Trying to register the namespace 'xml' must throw a NamespaceException.");
        } catch (NamespaceException ex) {
            // expected
        }
    }

    @Test
    public void testInvalidNamespace() throws Exception {
        final ContentSession session = createContentSession();
        final Root root = session.getLatestRoot();
        NamespaceRegistry r = getNamespaceRegistry(session, root);

        LogCustomizer customLogs = LogCustomizer.forLogger("org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry").enable(Level.ERROR).create();
        try {
            customLogs.starting();
            r.registerNamespace("foo", "example.com");
            r.unregisterNamespace("foo");
            List<String> myLogs = customLogs.getLogs();
            assertEquals(1, myLogs.size());
            assertTrue(myLogs.get(0).contains("Registering invalid namespace name 'example.com' for prefix 'foo', please see"));
        }
        finally {
            customLogs.finished();
        }
    }

    private static NamespaceRegistry getNamespaceRegistry(ContentSession session, Root root) {
        return new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return session.getLatestRoot();
            }
            @Override
            protected void refresh() {
                root.refresh();
            }
        };
    }
}
