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

package org.apache.jackrabbit.oak.spi.commit;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler.Resolution.IGNORED;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_PROPERTY;

import java.util.LinkedList;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.collections.ListUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * A {@code CompositeConflictHandler} delegates conflict handling
 * to multiple backing handlers. The backing handlers are invoked in
 * the inverse order they have been installed until a handler returning
 * a valid resolution (i.e. not {@code IGNORED)} is found. If for a certain
 * conflict none of the backing handlers returns a valid resolution
 * this implementation throws an {@code IllegalStateException}.
 */
public class CompositeConflictHandler implements ThreeWayConflictHandler {
    private final LinkedList<ThreeWayConflictHandler> handlers;

    /**
     * Create a new {@code CompositeConflictHandler} with an initial set of
     * backing handler. Use {@link #addHandler(ThreeWayConflictHandler)} to add additional
     * handlers.
     * @param handlers  the backing handlers
     */
    public CompositeConflictHandler(@NotNull Iterable<ThreeWayConflictHandler> handlers) {
        this.handlers = (LinkedList<ThreeWayConflictHandler>) ListUtils.toLinkedList(requireNonNull(handlers));
    }

    /**
     * Create a new {@code CompositeConflictHandler} with no backing handlers.
     * backing handler. Use {@link #addHandler(ThreeWayConflictHandler)} to add handlers.
     */
    public CompositeConflictHandler() {
        this.handlers = new LinkedList<>();
    }

    /**
     * Add a new backing conflict handler. The new handler takes precedence
     * over all currently installed handlers.
     * @param handler
     * @return this
     */
    public CompositeConflictHandler addHandler(@NotNull ThreeWayConflictHandler handler) {
        handlers.addFirst(handler);
        return this;
    }

    @NotNull
    @Override
    public Resolution addExistingProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.addExistingProperty(parent, ours, theirs);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                ADD_EXISTING_PROPERTY + " conflict");
    }

    @NotNull
    @Override
    public Resolution changeDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.changeDeletedProperty(parent, ours, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                CHANGE_DELETED_PROPERTY + " conflict");
    }

    @NotNull
    @Override
    public Resolution changeChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs,
                                            @NotNull PropertyState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.changeChangedProperty(parent, ours, theirs, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                CHANGE_CHANGED_PROPERTY + " conflict");
    }

    @NotNull
    @Override
    public Resolution deleteDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.deleteDeletedProperty(parent, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                DELETE_DELETED_PROPERTY + " conflict");
    }

    @NotNull
    @Override
    public Resolution deleteChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState theirs, @NotNull PropertyState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.deleteChangedProperty(parent, theirs, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                DELETE_CHANGED_PROPERTY + " conflict");
    }

    @NotNull
    @Override
    public Resolution addExistingNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours, @NotNull NodeState theirs) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.addExistingNode(parent, name, ours, theirs);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                ADD_EXISTING_NODE + " conflict");
    }

    @NotNull
    @Override
    public Resolution changeDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours, @NotNull NodeState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.changeDeletedNode(parent, name, ours, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                CHANGE_DELETED_NODE + " conflict");
    }

    @NotNull
    @Override
    public Resolution deleteChangedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState theirs, @NotNull NodeState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.deleteChangedNode(parent, name, theirs, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                DELETE_CHANGED_NODE + " conflict");
    }

    @NotNull
    @Override
    public Resolution deleteDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState base) {
        for (ThreeWayConflictHandler handler : handlers) {
            Resolution resolution = handler.deleteDeletedNode(parent, name, base);
            if (resolution != IGNORED) {
                return resolution;
            }
        }
        throw new IllegalStateException("No conflict handler for " +
                DELETE_DELETED_NODE + " conflict");
    }
}
