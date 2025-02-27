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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class NodeTypePatternTest extends AbstractSecurityTest {

    private final Set<String> ntNames = Set.of(JcrConstants.NT_UNSTRUCTURED, JcrConstants.NT_FOLDER);

    private final NodeTypePattern pattern = new NodeTypePattern(ntNames);

    @Test
    public void testMatchesItem() throws Exception {

        Tree rootTree = root.getTree("/");
        for (String ntName : ntNames) {
            Tree testTree = TreeUtil.addChild(rootTree, "name", ntName);

            assertTrue(pattern.matches(testTree, null));
            assertTrue(pattern.matches(testTree, PropertyStates.createProperty("a", Boolean.FALSE)));
            assertTrue(pattern.matches(testTree, PropertyStates.createProperty("f", "anyval")));

            testTree.remove();
        }
    }

    @Test
    public void testNotMatchesItem() throws Exception {
        Tree rootTree = root.getTree("/");

        List<String> notMatching = List.of(NodeTypeConstants.NT_OAK_RESOURCE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, JcrConstants.NT_VERSION);
        for (String ntName : notMatching) {
            Tree testTree = TreeUtil.addChild(rootTree, "name", ntName);

            assertFalse(pattern.matches(testTree, null));
            assertFalse(pattern.matches(testTree, PropertyStates.createProperty("a", Boolean.FALSE)));
            assertFalse(pattern.matches(testTree, PropertyStates.createProperty("f", "anyval")));

            testTree.remove();
        }
    }

    @Test
    public void testMatchesPath() {
        List<String> notMatching = List.of("/", "/a", "/b", "/c", "/d/e/a", "/a/b/c/d/b", "/test/c", "/d", "/b/d", "/d/e/f", "/c/b/abc");
        for (String p : notMatching) {
            assertFalse(pattern.matches(p));
        }
    }

    @Test
    public void testMatchesNull() {
        assertFalse(pattern.matches());
    }

    @Test
    public void testToString() {
        assertEquals(ntNames.toString(), pattern.toString());
    }

    @Test
    public void testHashCode() {
        assertEquals(ntNames.hashCode(), pattern.hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(pattern, pattern);
        assertEquals(pattern, new NodeTypePattern(ntNames));
    }

    @Test
    public void testNotEquals() {
        assertNotEquals(pattern, new NodeTypePattern(Set.of(JcrConstants.NT_UNSTRUCTURED)));
        assertNotEquals(pattern, new NodeTypePattern(Set.of(JcrConstants.NT_UNSTRUCTURED, JcrConstants.NT_FILE)));
        assertNotEquals(pattern, new NodeTypePattern(Set.of(JcrConstants.NT_VERSION)));
        assertNotEquals(pattern, new ItemNamePattern(ntNames));
    }

}