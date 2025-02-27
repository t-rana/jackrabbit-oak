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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.guava.common.base.Suppliers.memoize;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexUtil.encode;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index editor for keeping a property index up to date.
 * 
 * @see PropertyIndex
 * @see PropertyIndexLookup
 */
class PropertyIndexEditor implements IndexEditor {

    private static final Logger log = LoggerFactory.getLogger(PropertyIndexEditor.class);

    /** Parent editor, or {@code null} if this is the root editor. */
    private final PropertyIndexEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /** Index definition node builder */
    private final NodeBuilder definition;

    /** Root node state */
    private final NodeState root;

    private final Set<String> propertyNames;
    
    private final ValuePattern valuePattern;

    /** Type predicate, or {@code null} if there are no type restrictions */
    private final Predicate<NodeState> typePredicate;

    /**
     * This field is only set for unique indexes. Otherwise it is null.
     * Keys to check for uniqueness, or {@code null} for no uniqueness checks.
     */
    private final Set<String> keysToCheckForUniqueness;

    /**
     * Flag to indicate whether the type of this node may have changed.
     */
    private boolean typeChanged;

    /**
     * Matching property value keys from the before state. Lazily initialized.
     */
    private Set<String> beforeKeys;

    /**
     * Matching property value keys from the after state. Lazily initialized.
     */
    private Set<String> afterKeys;

    private final IndexUpdateCallback updateCallback;

    private final PathFilter pathFilter;

    private final PathFilter.Result pathFilterResult;

    private final MountInfoProvider mountInfoProvider;

    public PropertyIndexEditor(NodeBuilder definition, NodeState root,
                               IndexUpdateCallback updateCallback, MountInfoProvider mountInfoProvider) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;
        this.root = root;
        pathFilter = PathFilter.from(definition);
        pathFilterResult = getPathFilterResult();

        //initPropertyNames(definition);

        // get property names
        PropertyState names = definition.getProperty(PROPERTY_NAMES);
        if (names.count() == 1) { 
            // OAK-1273: optimize for the common case
            this.propertyNames = singleton(names.getValue(NAME, 0));
        } else {
            this.propertyNames = SetUtils.toSet(names.getValue(NAMES));
        }
        this.valuePattern = new ValuePattern(definition);

        // get declaring types, and all their subtypes
        // TODO: should we reindex when type definitions change?
        if (definition.hasProperty(DECLARING_NODE_TYPES)) {
            this.typePredicate = new TypePredicate(
                    root, definition.getNames(DECLARING_NODE_TYPES));
        } else {
            this.typePredicate = null;
        }

        // keep track of modified keys for uniqueness checks
        if (definition.getBoolean(IndexConstants.UNIQUE_PROPERTY_NAME)) {
            this.keysToCheckForUniqueness = new HashSet<>();
        } else {
            this.keysToCheckForUniqueness = null;
        }
        this.updateCallback = updateCallback;
        this.mountInfoProvider = mountInfoProvider;
    }
    
    PropertyIndexEditor(PropertyIndexEditor parent, String name, PathFilter.Result pathFilterResult) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.root = parent.root;
        this.propertyNames = parent.getPropertyNames();
        this.valuePattern = parent.valuePattern;
        this.typePredicate = parent.typePredicate;
        this.keysToCheckForUniqueness = parent.keysToCheckForUniqueness;
        this.updateCallback = parent.updateCallback;
        this.pathFilter = parent.pathFilter;
        this.pathFilterResult = pathFilterResult;
        this.mountInfoProvider = parent.mountInfoProvider;
    }
    
    /**
     * commodity method for allowing extensions
     * 
     * @return the propertyNames
     */
    Set<String> getPropertyNames() {
       return propertyNames;
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    private String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    /**
     * Adds the encoded values of the given property to the given set.
     * If the given set is uninitialized, i.e. {@code null}, then a new
     * set is created for any values to be added. The set, possibly newly
     * initialized, is returned.
     *
     * @param keys set of encoded values, or {@code null}
     * @param property property whose values are to be added to the set
     * @return set of encoded values, possibly initialized
     */
    private static Set<String> addValueKeys(
            Set<String> keys, PropertyState property, ValuePattern pattern) {
        if (property.getType().tag() != PropertyType.BINARY
                && property.count() > 0) {
            if (keys == null) {
                keys = new HashSet<>();
            }
            keys.addAll(encode(PropertyValues.create(property), pattern));
        }
        return keys;
    }

    private static Set<String> getMatchingKeys(
            NodeState state, Iterable<String> propertyNames, ValuePattern pattern) {
        Set<String> keys = null;
        for (String propertyName : propertyNames) {
            PropertyState property = state.getProperty(propertyName);
            if (property != null) {
                keys = addValueKeys(keys, property, pattern);
            }
        }
        return keys;
    }

    Set<IndexStoreStrategy> getStrategies(boolean unique) {
        return Multiplexers.getStrategies(unique, mountInfoProvider,
                definition, INDEX_CONTENT_NODE_NAME);
    }

    @Override
    public void enter(NodeState before, NodeState after) {
        // disables property name checks
        typeChanged = typePredicate == null; 
        
        beforeKeys = null;
        afterKeys = null;
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {

        if (pathFilterResult == PathFilter.Result.INCLUDE) {
            applyTypeRestrictions(before, after);
            updateIndex(before, after);
        }
        checkUniquenessConstraints();
        
    }
    
    private void applyTypeRestrictions(NodeState before, NodeState after) {
        // apply the type restrictions
        if (typePredicate != null) {
            if (typeChanged) {
                // possible type change, so ignore diff results and
                // just load all matching values from both states
                beforeKeys = getMatchingKeys(before, getPropertyNames(), valuePattern);
                afterKeys = getMatchingKeys(after, getPropertyNames(), valuePattern);
            }
            if (beforeKeys != null && !typePredicate.test(before)) {
                // the before state doesn't match the type, so clear its values
                beforeKeys = null;
            }
            if (afterKeys != null && !typePredicate.test(after)) {
                // the after state doesn't match the type, so clear its values
                afterKeys = null;
            }
        }
    }
    
    private void updateIndex(NodeState before, NodeState after) throws CommitFailedException {
        // if any changes were detected, update the index accordingly
        if (beforeKeys != null || afterKeys != null) {
            // first make sure that both the before and after sets are non-null
            if (beforeKeys == null
                    || (typePredicate != null && !typePredicate.test(before))) {
                beforeKeys = new HashSet<>();
            } else if (afterKeys == null) {
                afterKeys = new HashSet<>();
            } else {
                // both before and after matches found, remove duplicates
                Set<String> sharedKeys = new HashSet<>(beforeKeys);
                sharedKeys.retainAll(afterKeys);
                beforeKeys.removeAll(sharedKeys);
                afterKeys.removeAll(sharedKeys);
            }

            if (!beforeKeys.isEmpty() || !afterKeys.isEmpty()) {
                updateCallback.indexUpdate();
                String properties = definition.getString(PROPERTY_NAMES);
                boolean uniqueIndex = keysToCheckForUniqueness != null;
                for (IndexStoreStrategy strategy : getStrategies(uniqueIndex)) {
                    String indexNodeName = strategy.getIndexNodeName();
                    Supplier<NodeBuilder> index = memoize(() -> definition.child(indexNodeName));
                    if (uniqueIndex) {
                        Supplier<NodeBuilder> roBuilder;
                        if (definition.hasChildNode(indexNodeName)) {
                            roBuilder = index;
                        } else {
                            roBuilder = () -> EMPTY_NODE.builder();
                        }
                        keysToCheckForUniqueness.addAll(getExistingKeys(
                                afterKeys, roBuilder, strategy));
                    }
                    strategy.update(index, getPath(), properties, definition,
                            beforeKeys, afterKeys);
                }
            }
        }

        checkUniquenessConstraints();
    }

    private void checkUniquenessConstraints() throws CommitFailedException {
        if (parent == null) {
            // make sure that the index node exist, even with no content
            definition.child(INDEX_CONTENT_NODE_NAME);

            boolean uniqueIndex = keysToCheckForUniqueness != null;
            // check uniqueness constraints when leaving the root
            if (uniqueIndex &&
                    !keysToCheckForUniqueness.isEmpty()) {
                NodeState indexMeta = definition.getNodeState();
                String failed = getFirstDuplicate(
                        keysToCheckForUniqueness, indexMeta);
                if (failed != null) {
                    String msg = String.format(
                            "Uniqueness constraint violated property %s having value %s",
                            propertyNames, failed);
                    log.warn("checkUniquenessConstraints: {}", msg);
                    throw new CommitFailedException(CONSTRAINT, 30, msg);
                }
            }
        }
    }

    /**
     * From a set of keys, get those that already exist in the index.
     * 
     * @param keys the keys
     * @param index the index
     * @param s the index store strategy
     * @return the set of keys that already exist in this unique index
     */
    private Set<String> getExistingKeys(Set<String> keys, Supplier<NodeBuilder> index, IndexStoreStrategy s) {
        Set<String> existing = null;
        for (String key : keys) {
            if (s.exists(index, key)) {
                if (existing == null) {
                    existing = new HashSet<>();
                }
                existing.add(key);
            }
        }
        if (existing == null) {
            existing = Collections.emptySet();
        }
        return existing;
    }

    /**
     * From a set of keys, get the first that has multiple entries, if any.
     * 
     * @param keys the keys
     * @param indexMeta the index configuration
     * @return the first duplicate, or null if none was found
     */
    private String getFirstDuplicate(Set<String> keys, NodeState indexMeta) {
        for (String key : keys) {
            long count = 0;
            for (IndexStoreStrategy s : getStrategies(true)) {
                count += s.count(root, indexMeta, singleton(key), 2);
                if (count > 1) {
                    Iterator<String> it = s.query(null, null, indexMeta, singleton(key)).iterator();
                    if (it.hasNext()) {
                        return key + ": " + it.next();
                    }
                    return key;
                }
            }
        }
        return null;
    }

    private static boolean isTypeProperty(String name) {
        return JCR_PRIMARYTYPE.equals(name) || JCR_MIXINTYPES.equals(name);
    }

    @Override
    public void propertyAdded(PropertyState after) {
        String name = after.getName();
        typeChanged = typeChanged || isTypeProperty(name);
        if (getPropertyNames().contains(name)) {
            afterKeys = addValueKeys(afterKeys, after, valuePattern);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        String name = after.getName();
        typeChanged = typeChanged || isTypeProperty(name);
        if (getPropertyNames().contains(name)) {
            beforeKeys = addValueKeys(beforeKeys, before, valuePattern);
            afterKeys = addValueKeys(afterKeys, after, valuePattern);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        String name = before.getName();
        typeChanged = typeChanged || isTypeProperty(name);
        if (getPropertyNames().contains(name)) {
            beforeKeys = addValueKeys(beforeKeys, before, valuePattern);
        }
    }

    /**
     * Retrieve a new index editor associated with the child node to process
     * 
     * @param parent the index editor related to the parent node
     * @param name the name of the child node
     * @return an instance of the PropertyIndexEditor
     */
    PropertyIndexEditor getChildIndexEditor(@NotNull PropertyIndexEditor parent, @NotNull String name, PathFilter.Result filterResult) {
       return new PropertyIndexEditor(parent, name, filterResult);
    }
    
    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult == PathFilter.Result.EXCLUDE) {
            return null;
        }
        return getChildIndexEditor(this, name, filterResult);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult == PathFilter.Result.EXCLUDE) {
            return null;
        }
        return getChildIndexEditor(this, name, filterResult);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult == PathFilter.Result.EXCLUDE) {
            return null;
        }
        return getChildIndexEditor(this, name, filterResult);
    }

    private PathFilter.Result getPathFilterResult() {
        return pathFilter.filter(getPath());
    }

    private PathFilter.Result getPathFilterResult(String childNodeName) {
        return pathFilter.filter(concat(getPath(), childNodeName));
    }
}
