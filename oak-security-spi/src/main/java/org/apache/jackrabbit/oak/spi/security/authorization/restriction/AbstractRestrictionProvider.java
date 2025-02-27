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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRestrictionProvider implements RestrictionProvider, AggregationAware, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(AbstractRestrictionProvider.class);

    private final Map<String, RestrictionDefinition> supported;
    private CompositeRestrictionProvider composite = null;

    public AbstractRestrictionProvider(@NotNull Map<String, ? extends RestrictionDefinition> definitions) {
        Map<String, RestrictionDefinition> builder = new LinkedHashMap<>();
        builder.putAll(definitions);
        this.supported = Collections.unmodifiableMap(builder);
    }

    //---------------------------------------------------< AggregationAware >---
    @Override
    public void setComposite(@NotNull CompositeRestrictionProvider composite) {
        this.composite = composite;
    }

    //------------------------------------------------< RestrictionProvider >---
    @NotNull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
        if (isUnsupportedPath(oakPath)) {
            return Collections.emptySet();
        } else {
            return SetUtils.toLinkedSet(supported.values());
        }
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value value) throws RepositoryException {
        RestrictionDefinition definition = getDefinition(oakPath, oakName);
        Type<?> requiredType = definition.getRequiredType();
        int tag = requiredType.tag();
        if (tag != PropertyType.UNDEFINED && tag != value.getType()) {
            throw new AccessControlException("Unsupported restriction: Expected value of type " + requiredType + " for " + oakName);
        }
        PropertyState propertyState;
        if (requiredType.isArray()) {
            propertyState = PropertyStates.createProperty(oakName, List.of(value), tag);
        } else {
            propertyState = PropertyStates.createProperty(oakName, value);
        }
        return createRestriction(propertyState, definition);
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value... values) throws RepositoryException {
        RestrictionDefinition definition = getDefinition(oakPath, oakName);
        Type<?> requiredType = definition.getRequiredType();
        for (Value v : values) {
            if (requiredType.tag() != PropertyType.UNDEFINED && requiredType.tag() != v.getType()) {
                throw new AccessControlException("Unsupported restriction: Expected value of type " + requiredType + " for " + oakName);
            }
        }

        PropertyState propertyState;
        if (requiredType.isArray()) {
            propertyState = PropertyStates.createProperty(oakName, Arrays.asList(values), requiredType.tag());
        } else {
            if (values.length != 1) {
                throw new AccessControlException("Unsupported restriction: Expected single value for " + oakName);
            }
            propertyState = PropertyStates.createProperty(oakName, values[0]);
        }
        return createRestriction(propertyState, definition);
    }

    @NotNull
    @Override
    public Set<Restriction> readRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) {
        if (isUnsupportedPath(oakPath)) {
            return Collections.emptySet();
        } else {
            Set<Restriction> restrictions = new HashSet<>();
            for (PropertyState propertyState : getRestrictionsTree(aceTree).getProperties()) {
                String propName = propertyState.getName();
                if (isRestrictionProperty(propName)) {
                    RestrictionDefinition def = supported.get(propName);
                    if (def != null) {
                        // supported by this provider -> verify that type matches.
                        if (def.getRequiredType() == propertyState.getType()) {
                            restrictions.add(createRestriction(propertyState, def));
                        } else {
                            log.warn("Found restriction '{}' with type mismatch: expected '{}', found '{}'. It will be ignored.", propName, def.getRequiredType(), propertyState.getType());
                        }
                    } else {
                        // not supported by this provider. to properly deal with aggregation of multiple providers
                        // only log a warning if it is not supported by any other provider
                        if (!isSupportedByAnotherProvider(oakPath, propName)) {
                            log.warn("Unsupported restriction '{}' detected at {}. It will be ignored.", propName, oakPath);
                        }
                    }
                }
            }
            return restrictions;
        }
    }

    @Override
    public void writeRestrictions(@Nullable String oakPath, @NotNull Tree aceTree, @NotNull Set<Restriction> restrictions) throws RepositoryException {
        // validation of the restrictions is delegated to the commit hook
        // see #validateRestrictions below
        if (!restrictions.isEmpty()) {
            Tree rTree = TreeUtil.getOrAddChild(aceTree, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
            for (Restriction restriction : restrictions) {
                rTree.setProperty(restriction.getProperty());
            }
        }
    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) throws AccessControlException {
        Map<String, PropertyState> restrictionProperties = getRestrictionProperties(aceTree);
        if (isUnsupportedPath(oakPath)) {
            // test if there are any restrictions present that are not supported by another provider when used in a composite
            Set<String> unsupportedNames = restrictionProperties.keySet().stream().filter(name -> !isSupportedByAnotherProvider(oakPath, name)).collect(Collectors.toSet());
            if (!unsupportedNames.isEmpty()) {
                throw new AccessControlException("Restrictions '"+unsupportedNames+"' not supported with path '"+oakPath+"'.");
            }
        } else {
            // supported path -> validate restrictions and test if mandatory
            // restrictions are present.
            for (Map.Entry<String, PropertyState> entry : restrictionProperties.entrySet()) {
                String restrName = entry.getKey();
                RestrictionDefinition def = supported.get(restrName);
                if (def != null) {
                    // supported by this provider => verify matching type
                    Type<?> type = entry.getValue().getType();
                    if (type != def.getRequiredType()) {
                        throw new AccessControlException("Invalid restriction type '" + type + "' for " + restrName + ". Expected " + def.getRequiredType());
                    }
                } else {
                    // not supported by this provider. to properly deal with aggregation of multiple providers
                    // only throw exception if it is not supported by any other provider
                    if (!isSupportedByAnotherProvider(oakPath, restrName)) {
                        throw new AccessControlException("Unsupported restriction: " + restrName);
                    }
                }
            }
            for (RestrictionDefinition def : supported.values()) {
                if (def.isMandatory() && !restrictionProperties.containsKey(def.getName())) {
                    throw new AccessControlException("Mandatory restriction " + def.getName() + " is missing.");
                }
            }
        }
    }

    //----------------------------------------------------------< protected >---
    /**
     * Returns {@code true} if the specified path is {@code null}. Subclasses may
     * change the default behavior.
     *
     * @param oakPath The path for which a restriction is being created.
     * @return {@code true} if this implementation can create restrictions for
     * the specified {@code oakPath}; {@code false} otherwise.
     */
    protected boolean isUnsupportedPath(@Nullable String oakPath) {
        return oakPath == null;
    }

    /**
     * Returns the tree that contains the restriction of the specified
     * ACE tree.
     *
     * @param aceTree The ACE tree for which the restrictions are being read.
     * @return The tree storing the restriction information.
     */
    @NotNull
    protected Tree getRestrictionsTree(@NotNull Tree aceTree) {
        Tree restrictions = aceTree.getChild(REP_RESTRICTIONS);
        if (!restrictions.exists()) {
            // no rep:restrictions tree -> read from aceTree for backwards compatibility
            restrictions = aceTree;
        }
        return restrictions;
    }

    //------------------------------------------------------------< private >---
    @NotNull
    private RestrictionDefinition getDefinition(@Nullable String oakPath, @NotNull String oakName) throws AccessControlException {
        if (isUnsupportedPath(oakPath)) {
            throw new AccessControlException("Unsupported restriction at " + oakPath);
        }
        RestrictionDefinition definition = supported.get(oakName);
        if (definition == null) {
            throw new AccessControlException("Unsupported restriction: " + oakName);
        }
        return definition;
    }

    /**
     * Evaluate if a restriction with the given name at the given path is supported by another restriction provider 
     * in case this instance is used in a {@link CompositeRestrictionProvider}.
     * 
     * @param oakPath The oak path
     * @param oakName The name of the restriction
     * @return {@code true} if this provider is used inside a composite {@code RestrictionProvider} which 
     * supports restrictions with the given name; {@code false} otherwise or if this provider instance is not aggregated 
     * inside a composite.
     */
    private boolean isSupportedByAnotherProvider(@Nullable String oakPath, @NotNull String oakName) {
        if (composite != null) {
            return composite.getSupportedRestrictions(oakPath).stream().anyMatch(restrictionDefinition -> restrictionDefinition.getName().equals(oakName));
        } else {
            // not used inside a composite -> therefore it's not supported
            return false;
        }
    }

    @NotNull
    private static Restriction createRestriction(@NotNull PropertyState propertyState, @NotNull RestrictionDefinition definition) {
        return new RestrictionImpl(propertyState, definition);
    }

    @NotNull
    private Map<String, PropertyState> getRestrictionProperties(@NotNull Tree aceTree) {
        Tree rTree = getRestrictionsTree(aceTree);
        Map<String, PropertyState> restrictionProperties = new HashMap<>();
        for (PropertyState property : rTree.getProperties()) {
            String name = property.getName();
            if (isRestrictionProperty(name)) {
                restrictionProperties.put(name, property);
            }
        }
        return restrictionProperties;
    }

    private static boolean isRestrictionProperty(@NotNull String propertyName) {
        return !AccessControlConstants.ACE_PROPERTY_NAMES.contains(propertyName) &&
                !NamespaceRegistry.PREFIX_JCR.equals(Text.getNamespacePrefix(propertyName));
    }
}
