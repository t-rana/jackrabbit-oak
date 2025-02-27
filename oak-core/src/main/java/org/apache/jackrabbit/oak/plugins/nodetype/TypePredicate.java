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
package org.apache.jackrabbit.oak.plugins.nodetype;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.jackrabbit.guava.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.collections.StreamUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PRIMARY_SUBTYPES;

/**
 * Inheritance-aware node type predicate for {@link NodeState node states}.
 *
 * @since Oak 0.11
 */
public class TypePredicate implements Predicate<NodeState> {

    @NotNull
    public static TypePredicate isOrderable(@NotNull NodeState root) {
        Set<String> orderable = new HashSet<>();
        NodeState types = requireNonNull(root)
                .getChildNode(JCR_SYSTEM)
                .getChildNode(JCR_NODE_TYPES);
        for (ChildNodeEntry entry : types.getChildNodeEntries()) {
            NodeState type = entry.getNodeState();
            if (type.getBoolean(JCR_HASORDERABLECHILDNODES)) {
                orderable.add(entry.getName());
            }
        }
        return new TypePredicate(root, orderable);
    }

    private final NodeState root;

    private final Iterable<String> names;

    private boolean initialized;

    private Set<String> primaryTypes;

    private Set<String> mixinTypes;

    /**
     * Creates a predicate for checking whether a node state is an instance of
     * the named node type. This is an O(1) operation in terms of item
     * accesses.
     *
     * @param root root node state
     * @param name Oak name of the node type to check for
     */
    public TypePredicate(@NotNull NodeState root, @NotNull String name) {
        this(root, singleton(name));
    }

    /**
     * Creates a predicate for checking whether a node state is an instance of
     * any of the named node types. This is an O(n) operation in terms of item
     * accesses, with n being the number of given node types.
     *
     * @param root root node state
     * @param names Oak names of the node types to check for
     */
    public TypePredicate(@NotNull NodeState root, @NotNull Iterable<String> names) {
        this.root = root;
        this.names = names;
    }

    /**
     * Creates a predicate for checking whether a node state is an instance of
     * any of the named node types. This is an O(n) operation in terms of item
     * accesses, with n being the number of given node types.
     *
     * @param root root node state
     * @param names Oak names of the node types to check for
     */
    public TypePredicate(@NotNull NodeState root, @NotNull String[] names) {
        this(root, Arrays.asList(names));
    }

    private static Set<String> add(Set<String> names, String name) {
        if (names == null) {
            return SetUtils.toSet(name);
        } else {
            names.add(name);
            return names;
        }
    }

    private void addNodeType(NodeState types, String name) {
        NodeState type = types.getChildNode(name);

        for (String primary : type.getNames(REP_PRIMARY_SUBTYPES)) {
            primaryTypes = add(primaryTypes, primary);
        }

        if (type.getBoolean(JCR_ISMIXIN)) {
            mixinTypes = add(mixinTypes, name);

            // Only mixin types can have mixin descendants, so we
            // only fill the mixinTypes set in this branch of code.
            for (String mixin : type.getNames(REP_MIXIN_SUBTYPES)) {
                mixinTypes = add(mixinTypes, mixin);
            }
        } else {
            // No need to check whether the type actually exists, as if
            // it doesn't there should in any case be no matching content.
            primaryTypes = add(primaryTypes, name);
        }
    }

    private void init() {
        if (!initialized) {
            // lazy initialization of the sets of matching type names
            NodeState types = requireNonNull(root)
                    .getChildNode(JCR_SYSTEM)
                    .getChildNode(JCR_NODE_TYPES);
            for (String name : requireNonNull(names)) {
                addNodeType(types, name);
            }
            initialized = true;
        }
    }

    public boolean test(String primary, Set<String> mixins) {
        init();
        if (primaryTypes != null && primaryTypes.contains(primary)) {
            return true;
        }
        if (mixinTypes != null && StreamUtils.toStream(mixins).anyMatch(mixinTypes::contains)) {
            return true;
        }
        return false;
    }

    public boolean test(@Nullable Tree input) {
        if (input != null) {
            init();
            if (primaryTypes != null
                    && primaryTypes.contains(TreeUtil.getPrimaryTypeName(input))) {
                return true;
            }
            if (mixinTypes != null
                    && StreamUtils.toStream(TreeUtil.getNames(input, JCR_MIXINTYPES)).anyMatch(mixinTypes::contains)) {
                return true;
            }
        }
        return false;
    }

    //---------------------------------------------------------< Predicate >--

    @Override
    public boolean test(@Nullable NodeState input) {
        if (input != null) {
            init();
            if (primaryTypes != null
                    && primaryTypes.contains(input.getName(JCR_PRIMARYTYPE))) {
                return true;
            }
            if (mixinTypes != null
                    && StreamUtils.toStream(input.getNames(JCR_MIXINTYPES)).anyMatch(mixinTypes::contains)) {
                return true;
            }
        }
        return false;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return Iterables.toString(names);
    }
}
