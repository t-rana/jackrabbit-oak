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
package org.apache.jackrabbit.oak.plugins.index.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.collect.ArrayListMultimap;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.ListMultimap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate.NodeInclude;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate.NodeIncludeResult;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate.PropertyIncludeResult;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.guava.common.collect.Iterables.toArray;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class AggregateTest {

    private final TestCollector col = new TestCollector();
    private final SimpleMapper mapper = new SimpleMapper();
    private final NodeState root = INITIAL_CONTENT;
    private final NodeBuilder builder = root.builder();

    //~---------------------------------< Node Includes >

    @Test
    public void oneLevelAll() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("*")));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").child("c");
        nb.child("b");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(2, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "b"));
    }

    @Test
    public void oneLevelNamed() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("a")));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a");
        nb.child("b");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(1, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a"));
    }

    @Test
    public void noOfChildNodeRead() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("a")));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a");
        for (int i = 0; i < 10; i++) {
            nb.child("a"+i);
        }

        NodeState state = nb.getNodeState();
        final AtomicInteger counter = new AtomicInteger();
        Iterable<? extends ChildNodeEntry> countingIterator = Iterables.transform(state.getChildNodeEntries(),
                input -> {
                    counter.incrementAndGet();
                    return input;
                });
        NodeState mocked = spy(state);
        doReturn(countingIterator).when(mocked).getChildNodeEntries();
        ag.collectAggregates(mocked, col);

        //Here at max a single call should happen for reading child nodes
        assertThat(counter.get(), is(lessThanOrEqualTo(1)));
    }

    @Test
    public void oneLevelTyped() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("nt:resource","*", false)));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").setProperty(JCR_PRIMARYTYPE,"nt:resource");
        nb.child("b");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(1, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a"));
    }

    @Test
    public void oneLevelTypedMixin() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("mix:title","*", false)));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").setProperty(JcrConstants.JCR_MIXINTYPES, Collections.singleton("mix:title"), Type.NAMES);
        nb.child("b");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(1, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a"));
    }

    @Test
    public void multiLevelAll() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("*"), ni("*/*")));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").child("c");
        nb.child("b");
        nb.child("d").child("e").child("f");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(5, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "b", "d", "a/c", "d/e"));
    }

    @Test
    public void multiLevelNamed() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("a"), ni("d/e")));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").child("c");
        nb.child("b");
        nb.child("d").child("e").child("f");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(2, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "d/e"));
    }

    @Test
    public void multiLevelTyped() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("a"),
                ni("nt:resource", "d/*/*", false)));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").child("c");
        nb.child("b");
        nb.child("d").child("e").child("f").setProperty(JCR_PRIMARYTYPE,"nt:resource");
        nb.child("d").child("e").child("f2");
        nb.child("d").child("e2").child("f3").setProperty(JCR_PRIMARYTYPE, "nt:resource");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(3, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "d/e/f", "d/e2/f3"));
    }

    @Test
    public void multiLevelNamedSubAll() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("a"), ni("d/*/*")));
        NodeBuilder nb = newNode("nt:base");
        nb.child("a").child("c");
        nb.child("b");
        nb.child("d").child("e").child("f");
        nb.child("d").child("e").child("f2");
        nb.child("d").child("e2").child("f3");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(4, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "d/e/f", "d/e/f2", "d/e2/f3"));
    }

    //~---------------------------------< Node include recursive >

    @Test
    public void multiAggregateMapping() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni("*")));

        Aggregate agFile = new Aggregate("nt:file", List.of(ni("*"), ni("*/*")));
        mapper.add("nt:file", agFile);

        NodeBuilder nb = newNode("nt:base");
        nb.child("a").child("c");
        nb.child("b").setProperty(JCR_PRIMARYTYPE, "nt:file");
        nb.child("b").child("b1").child("b2");
        nb.child("c");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(5, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "b", "c", "b/b1", "b/b1/b2"));
    }

    @Test
    public void recursionEnabled() {
        Aggregate agFile = new Aggregate("nt:file", List.of(ni("*")), 5);
        mapper.add("nt:file", agFile);

        NodeBuilder nb = newNode("nt:file");
        nb.child("a").child("c");
        nb.child("b").setProperty(JCR_PRIMARYTYPE, "nt:file");
        nb.child("b").child("b1").child("b2");
        nb.child("c");

        agFile.collectAggregates(nb.getNodeState(), col);
        assertEquals(4, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("a", "b", "c", "b/b1"));
    }

    @Test
    public void recursionEnabledWithLimitCheck() {
        int limit = 5;
        Aggregate agFile = new Aggregate("nt:file", List.of(ni("*")), limit);
        mapper.add("nt:file", agFile);

        List<String> expectedPaths = new ArrayList<>();
        NodeBuilder nb = newNode("nt:file");
        nb.child("a").child("c");

        String path = "";
        NodeBuilder fb = nb;
        for (int i = 0; i < limit + 2; i++){
            String name = "f "+ i;
            path = PathUtils.concat(path, name);
            fb = fb.child(name);
            fb.setProperty(JCR_PRIMARYTYPE, "nt:file");

            if (i < limit) {
                expectedPaths.add(path);
            }
        }
        expectedPaths.add("a");

        agFile.collectAggregates(nb.getNodeState(), col);
        assertEquals(expectedPaths.size(), col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems(toArray(expectedPaths, String.class)));
    }

    @Test
    public void includeMatches() {
        Aggregate ag = new Aggregate("nt:base", List.of(ni(null, "*", true), ni(null, "*/*", true)));
        assertTrue(ag.hasRelativeNodeInclude("foo"));
        assertTrue(ag.hasRelativeNodeInclude("foo/bar"));
        assertFalse(ag.hasRelativeNodeInclude("foo/bar/baz"));

        Aggregate ag2 = new Aggregate("nt:base", List.of(ni(null, "foo", true), ni(null, "foo/*", true)));
        assertTrue(ag2.hasRelativeNodeInclude("foo"));
        assertFalse(ag2.hasRelativeNodeInclude("bar"));
        assertTrue(ag2.hasRelativeNodeInclude("foo/bar"));
        assertFalse(ag2.hasRelativeNodeInclude("foo/bar/baz"));
    }

    @Test
    public void testReaggregate() {
        //Enable relative include for all child nodes of nt:folder
        //So indexing would create fulltext field for each relative nodes
        Aggregate agFolder = new Aggregate("nt:folder", List.of(ni("nt:file", "*", true)));

        Aggregate agFile = new Aggregate("nt:file", List.of(ni(null, "jcr:content", true)));
        mapper.add("nt:file", agFile);
        mapper.add("nt:folder", agFolder);

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").child("c");
        createFile(nb, "b", "hello world");
        createFile(nb, "c", "hello world");

        agFolder.collectAggregates(nb.getNodeState(), col);
        assertEquals(4, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("b", "c", "b/jcr:content", "c/jcr:content"));

        assertEquals(2, col.nodeResults.get("b/jcr:content").size());

        //Check that a result is provided for relative node 'b'. Actual node provided
        //is b/jcr:content
        assertEquals(1, col.getRelativeNodeResults("b/jcr:content", "b").size());
    }

    @Test
    public void testReaggregateMixin() {
        //A variant of testReaggregation but using mixin
        //instead of normal nodetype. It abuses mix:title
        //and treat it like nt:file. Test check if reaggregation
        //works for mixins also

        //Enable relative include for all child nodes of nt:folder
        //So indexing would create fulltext field for each relative nodes
        Aggregate agFolder = new Aggregate("nt:folder", List.of(ni("mix:title", "*", true)));

        Aggregate agFile = new Aggregate("mix:title", List.of(ni(null, "jcr:content", true)));
        mapper.add("mix:title", agFile);
        mapper.add("nt:folder", agFolder);

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").child("c");
        createFileMixin(nb, "b", "hello world");
        createFileMixin(nb, "c", "hello world");

        agFolder.collectAggregates(nb.getNodeState(), col);
        assertEquals(4, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("b", "c", "b/jcr:content", "c/jcr:content"));

        assertEquals(2, col.nodeResults.get("b/jcr:content").size());

        //Check that a result is provided for relative node 'b'. Actual node provided
        //is b/jcr:content
        assertEquals(1, col.getRelativeNodeResults("b/jcr:content", "b").size());
    }

    @Test
    public void testRelativeNodeInclude() {
        //Enable relative include for all child nodes of nt:folder
        //So indexing would create fulltext field for each relative nodes
        Aggregate agContent = new Aggregate("app:Page", List.of(ni(null, "jcr:content", true)));

        mapper.add("app:Page", agContent);

        NodeBuilder nb = newNode("app:Page");
        nb.child("jcr:content").setProperty("foo", "bar");

        agContent.collectAggregates(nb.getNodeState(), col);
        assertEquals(1, col.getNodePaths().size());
        assertThat(col.getNodePaths(), hasItems("jcr:content"));

        assertEquals(2, col.nodeResults.get("jcr:content").size());

        //Check that a result is provided for relative node 'b'. Actual node provided
        //is b/jcr:content
        assertEquals(1, col.getRelativeNodeResults("jcr:content", "jcr:content").size());
    }

    private static void createFile(NodeBuilder nb, String fileName, String content){
        nb.child(fileName).setProperty(JCR_PRIMARYTYPE, "nt:file")
                .child("jcr:content").setProperty("jcr:data", content.getBytes());
    }

    private static void createFileMixin(NodeBuilder nb, String fileName, String content){
        //Abusing mix:title as it's registered by default
        nb.child(fileName).setProperty(JCR_MIXINTYPES, Collections.singleton("mix:title"), Type.NAMES)
                .child("jcr:content").setProperty("jcr:data", content.getBytes());
    }

    //~---------------------------------< Prop Includes >

    @Test
    public void propOneLevelNamed() {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/p1")
                .setProperty(FulltextIndexConstants.PROP_NAME, "a/p1");

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Aggregate ag = defn.getApplicableIndexingRule("nt:folder").getAggregate();

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").setProperty("p1", "foo");
        nb.child("a").setProperty("p2", "foo");
        nb.child("b").setProperty("p2", "foo");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(1, col.getPropPaths().size());
        assertThat(col.getPropPaths(), hasItems("a/p1"));
    }

    @Test
    public void propOneLevelRegex() {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/p1")
                .setProperty(FulltextIndexConstants.PROP_NAME, "a/foo.*")
                .setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Aggregate ag = defn.getApplicableIndexingRule("nt:folder").getAggregate();

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").setProperty("foo1", "foo");
        nb.child("a").setProperty("foo2", "foo");
        nb.child("a").setProperty("bar1", "foo");
        nb.child("b").setProperty("p2", "foo");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(2, col.getPropPaths().size());
        assertThat(col.getPropPaths(), hasItems("a/foo1", "a/foo2"));
    }

    @Test
    public void regexWithHiddenElements() {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/p1")
                .setProperty(FulltextIndexConstants.PROP_NAME, "a/.*")
                .setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Aggregate ag = defn.getApplicableIndexingRule("nt:folder").getAggregate();

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").setProperty("foo1", "foo");
        nb.child("a").setProperty("foo2", "foo");
        nb.child("a").setProperty(":hiddenProperty", "foo");
        nb.child("b").setProperty("p2", "foo");
        nb.child("a").child(":hiddenNode").setProperty("foo3", "foo");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(2, col.getPropPaths().size());
        assertThat(col.getPropPaths(), hasItems("a/foo1", "a/foo2"));
    }

    //~---------------------------------< IndexingConfig >

    @Test
    public void simpleAggregateConfig() {
        NodeBuilder aggregates = builder.child(FulltextIndexConstants.AGGREGATES);
        NodeBuilder aggFolder = aggregates.child("nt:folder");
        aggFolder.child("i1").setProperty(FulltextIndexConstants.AGG_PATH, "*");

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Aggregate agg = defn.getAggregate("nt:folder");
        assertNotNull(agg);
        assertEquals(1, agg.getIncludes().size());
    }

    @Test
    public void aggregateConfig2() {
        NodeBuilder aggregates = builder.child(FulltextIndexConstants.AGGREGATES);
        NodeBuilder aggFolder = aggregates.child("nt:folder");
        aggFolder.setProperty(FulltextIndexConstants.AGG_RECURSIVE_LIMIT, 42);
        aggFolder.child("i1").setProperty(FulltextIndexConstants.AGG_PATH, "*");
        aggFolder.child("i1").setProperty(FulltextIndexConstants.AGG_PRIMARY_TYPE, "nt:file");
        aggFolder.child("i1").setProperty(FulltextIndexConstants.AGG_RELATIVE_NODE, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Aggregate agg = defn.getAggregate("nt:folder");
        assertNotNull(agg);
        assertEquals(42, agg.reAggregationLimit);
        assertEquals(1, agg.getIncludes().size());
        assertEquals("nt:file", ((NodeInclude)agg.getIncludes().get(0)).primaryType);
        assertTrue(((NodeInclude)agg.getIncludes().get(0)).relativeNode);
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(Objects.requireNonNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    private Aggregate.Include ni(String pattern){
        return new NodeInclude(mapper, pattern);
    }

    private Aggregate.Include ni(String type, String pattern, boolean relativeNode){
        return new NodeInclude(mapper, type, pattern, relativeNode);
    }

    private static class TestCollector implements Aggregate.ResultCollector {
        final ListMultimap<String, NodeIncludeResult> nodeResults = ArrayListMultimap.create();
        final Map<String, PropertyIncludeResult> propResults = new HashMap<>();
        @Override
        public void onResult(NodeIncludeResult result) {
            nodeResults.put(result.nodePath, result);
        }

        @Override
        public void onResult(PropertyIncludeResult result) {
            propResults.put(result.propertyPath, result);

        }

        public Collection<String> getNodePaths(){
            return nodeResults.keySet();
        }

        public Collection<String> getPropPaths(){
            return propResults.keySet();
        }

        public void reset(){
            nodeResults.clear();
            propResults.clear();
        }

        public List<NodeIncludeResult> getRelativeNodeResults(String path, String rootIncludePath){
            List<NodeIncludeResult> result = new ArrayList<>();

            for (NodeIncludeResult nr : nodeResults.get(path)){
                if (rootIncludePath.equals(nr.rootIncludePath)){
                    result.add(nr);
                }
            }

            return result;
        }
    }

    private static class SimpleMapper implements Aggregate.AggregateMapper {
        final Map<String, Aggregate> mapping = new HashMap<>();

        @Override
        public Aggregate getAggregate(String nodeTypeName) {
            return mapping.get(nodeTypeName);
        }

        public void add(String type, Aggregate agg){
            mapping.put(type, agg);
        }
    }

}
