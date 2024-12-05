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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValues;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ElasticIndexDefinition extends IndexDefinition {

    public static final String TYPE_ELASTICSEARCH = "elasticsearch";

    public static final String BULK_ACTIONS = "bulkActions";
    public static final int BULK_ACTIONS_DEFAULT = 250;

    public static final String BULK_SIZE_BYTES = "bulkSizeBytes";
    public static final long BULK_SIZE_BYTES_DEFAULT = 1024 * 1024; // 1MB

    public static final String BULK_FLUSH_INTERVAL_MS = "bulkFlushIntervalMs";
    public static final long BULK_FLUSH_INTERVAL_MS_DEFAULT = 3000;

    public static final String NUMBER_OF_SHARDS = "numberOfShards";
    public static final int NUMBER_OF_SHARDS_DEFAULT = 1;

    public static final String NUMBER_OF_REPLICAS = "numberOfReplicas";
    public static final int NUMBER_OF_REPLICAS_DEFAULT = 1;

    public static final String QUERY_FETCH_SIZES = "queryFetchSizes";
    public static final Long[] QUERY_FETCH_SIZES_DEFAULT = new Long[]{10L, 100L, 1000L};

    public static final String QUERY_TIMEOUT_MS = "queryTimeoutMs";
    public static final long QUERY_TIMEOUT_MS_DEFAULT = 60000;

    public static final String TRACK_TOTAL_HITS = "trackTotalHits";
    public static final Integer TRACK_TOTAL_HITS_DEFAULT = 10000;

    /**
     * Hidden property for storing the index mapping version.
     */
    public static final String PROP_INDEX_MAPPING_VERSION = ":mappingVersion";

    public static final String DYNAMIC_MAPPING = "dynamicMapping";
    // possible values are: true, false, runtime, strict. See https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html
    public static final String DYNAMIC_MAPPING_DEFAULT = "true";

    // when true, fails indexing in case of bulk failures
    public static final String FAIL_ON_ERROR = "failOnError";
    public static final boolean FAIL_ON_ERROR_DEFAULT = true;

    /**
     * When 0, the index name gets dynamically generated by adding a random suffix to the index name.
     */
    public static final String INDEX_NAME_SEED = "indexNameSeed";
    public static final long INDEX_NAME_SEED_DEFAULT = 0L;

    /**
     * Hidden property for storing a seed value to be used as suffix in remote index name.
     */
    public static final String PROP_INDEX_NAME_SEED = ":nameSeed";

    /**
     * Hidden property to store similarity tags
     */
    public static final String SIMILARITY_TAGS = ":simTags";

    /**
     * Hidden property to handle dynamic tags for fulltext queries
     */
    public static final String DYNAMIC_BOOST_FULLTEXT = ":dynamic-boost-ft";

    /**
     * Precomputed random value based on the path of the document
     */
    public static final String PATH_RANDOM_VALUE = ":path-random-value";

    /**
     * Hidden property to store the last updated timestamp
     */
    public static final String LAST_UPDATED = ":lastUpdated";

    /**
     * Dynamic properties are fields that are not explicitly defined in the index mapping and are added on the fly when a document is indexed.
     * Examples: aggregations with relative nodes, regex properties (to be supported), etc.
     */
    public static final String DYNAMIC_PROPERTIES = ":dynamic-properties";

    public static final String SPLIT_ON_CASE_CHANGE = "splitOnCaseChange";
    public static final String SPLIT_ON_NUMERICS = "splitOnNumerics";

    public static final String INFERENCE = ":inference";

    private static final String SIMILARITY_TAGS_ENABLED = "similarityTagsEnabled";
    private static final boolean SIMILARITY_TAGS_ENABLED_DEFAULT = true;

    private static final String SIMILARITY_TAGS_FIELDS = "similarityTagsFields";

    // MLT queries, when no fields are specified, do not use the entire document but only a maximum of
    // max_query_terms (default 25). Even increasing this value, the query could produce not so relevant
    // results (eg: based on the :fulltext content). To work this around, we can specify DYNAMIC_BOOST_FULLTEXT
    // field with overridden mlt params and increased boost since it usually contains relevant terms. This will make sure
    // that the MLT queries give more priority to the terms in this field while the rest (*) are considered secondary.
    // TODO: we can further improve search relevance by using the actual tags combined with the weights using a function query.
    //      Right now, we are just matching the tags without looking at the weights. Therefore, a tag can be matched in a field with a lower weight.
    private static final String[] SIMILARITY_TAGS_FIELDS_DEFAULT = new String[] {
            "mlt.fl=" + DYNAMIC_BOOST_FULLTEXT + "&mlt.mintf=1&mlt.qf=3",
            "mlt.fl=*&mlt.mintf=2"
    };

    private static final String SIMILARITY_TAGS_BOOST = "similarityTagsBoost";
    private static final float SIMILARITY_TAGS_BOOST_DEFAULT = 0.5f;

    protected static final String INFERENCE_CONFIG = "inference";

    private static final Function<Integer, Boolean> isAnalyzable;

    static {
        int[] NOT_ANALYZED_TYPES = new int[] {
                Type.BINARY.tag(), Type.LONG.tag(), Type.DOUBLE.tag(), Type.DECIMAL.tag(), Type.DATE.tag(), Type.BOOLEAN.tag()
        };
        Arrays.sort(NOT_ANALYZED_TYPES); // need for binary search
        isAnalyzable = type -> Arrays.binarySearch(NOT_ANALYZED_TYPES, type) < 0;
    }

    /**
     * Mapping version that uses <a href="https://semver.org/">SemVer Specification</a> to allow changes without
     * breaking existing queries.
     * Changes breaking compatibility should increment the major version (indicating that a reindex is mandatory).
     * Changes not breaking compatibility should increment the minor version (old queries still work, but they might not
     * use the new feature).
     * Changes that do not affect queries should increment the patch version (eg: bug fixes).
     * <p>
     * WARN: Since this information might be needed from external tools that don't have a direct dependency on this module, the
     * actual version needs to be set in oak-search.
     */
    public static final ElasticSemVer MAPPING_VERSION;
    static {
        MAPPING_VERSION = ElasticSemVer.fromString(FulltextIndexConstants.INDEX_VERSION_BY_TYPE.get(ElasticIndexDefinition.TYPE_ELASTICSEARCH));
    }

    private final String indexPrefix;
    private final String indexAlias;
    public final int bulkActions;
    public final long bulkSizeBytes;
    public final long bulkFlushIntervalMs;
    private final boolean similarityTagsEnabled;
    private final float similarityTagsBoost;
    public final int numberOfShards;
    public final int numberOfReplicas;
    public final int[] queryFetchSizes;
    public final long queryTimeoutMs;
    public final Integer trackTotalHits;
    public final String dynamicMapping;
    public final boolean failOnError;
    public final long indexNameSeed;
    public final InferenceDefinition inferenceDefinition;

    private final Map<String, List<PropertyDefinition>> propertiesByName;
    private final List<PropertyDefinition> dynamicBoostProperties;
    private final List<PropertyDefinition> similarityProperties;
    private final List<PropertyDefinition> similarityTagsProperties;
    private final String[] similarityTagsFields;

    public ElasticIndexDefinition(NodeState root, NodeState defn, String indexPath, String indexPrefix) {
        super(root, defn, determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
        this.indexPrefix = indexPrefix;
        // the alias contains the internal mapping major version. In case of a breaking change, an index with the new version can
        // be created without affecting the existing queries of an instance running with the old version.
        // This strategy has been introduced from version 1. For compatibility reasons, the alias is not changed for the first version.
        if (MAPPING_VERSION.getMajor() > 1) {
            this.indexAlias = ElasticIndexNameHelper.getElasticSafeIndexName(indexPrefix, getIndexPath() + "_v" + MAPPING_VERSION.getMajor());
        } else {
            this.indexAlias = ElasticIndexNameHelper.getElasticSafeIndexName(indexPrefix, getIndexPath());
        }
        this.bulkActions = getOptionalValue(defn, BULK_ACTIONS, BULK_ACTIONS_DEFAULT);
        this.bulkSizeBytes = getOptionalValue(defn, BULK_SIZE_BYTES, BULK_SIZE_BYTES_DEFAULT);
        this.bulkFlushIntervalMs = getOptionalValue(defn, BULK_FLUSH_INTERVAL_MS, BULK_FLUSH_INTERVAL_MS_DEFAULT);
        this.numberOfShards = getOptionalValue(defn, NUMBER_OF_SHARDS, NUMBER_OF_SHARDS_DEFAULT);
        this.numberOfReplicas = getOptionalValue(defn, NUMBER_OF_REPLICAS, NUMBER_OF_REPLICAS_DEFAULT);
        this.similarityTagsEnabled = getOptionalValue(defn, SIMILARITY_TAGS_ENABLED, SIMILARITY_TAGS_ENABLED_DEFAULT);
        this.similarityTagsBoost = getOptionalValue(defn, SIMILARITY_TAGS_BOOST, SIMILARITY_TAGS_BOOST_DEFAULT);
        this.queryFetchSizes = Arrays.stream(getOptionalValues(defn, QUERY_FETCH_SIZES, Type.LONGS, Long.class, QUERY_FETCH_SIZES_DEFAULT))
                .mapToInt(Long::intValue).toArray();
        this.queryTimeoutMs = getOptionalValue(defn, QUERY_TIMEOUT_MS, QUERY_TIMEOUT_MS_DEFAULT);
        this.trackTotalHits = getOptionalValue(defn, TRACK_TOTAL_HITS, TRACK_TOTAL_HITS_DEFAULT);
        this.dynamicMapping = getOptionalValue(defn, DYNAMIC_MAPPING, DYNAMIC_MAPPING_DEFAULT);
        this.failOnError = getOptionalValue(defn, FAIL_ON_ERROR,
                Boolean.parseBoolean(System.getProperty(TYPE_ELASTICSEARCH + "." + FAIL_ON_ERROR, Boolean.toString(FAIL_ON_ERROR_DEFAULT)))
        );
        this.indexNameSeed = getOptionalValue(defn, INDEX_NAME_SEED, INDEX_NAME_SEED_DEFAULT);
        this.similarityTagsFields = getOptionalValues(defn, SIMILARITY_TAGS_FIELDS, Type.STRINGS, String.class, SIMILARITY_TAGS_FIELDS_DEFAULT);

        this.propertiesByName = getDefinedRules()
                .stream()
                .flatMap(rule -> Stream.concat(CollectionUtils.toStream(rule.getProperties()),
                        rule.getFunctionRestrictions().stream()))
                .filter(pd -> pd.index) // keep only properties that can be indexed
                .collect(Collectors.groupingBy(pd -> {
                    if (pd.function != null) {
                        return pd.function;
                    } else {
                        return pd.name;
                    }
                }));

        this.dynamicBoostProperties = getDefinedRules()
                .stream()
                .flatMap(IndexingRule::getNamePatternsProperties)
                .filter(pd -> pd.dynamicBoost)
                .collect(Collectors.toList());

        this.similarityProperties = getDefinedRules()
                .stream()
                .flatMap(rule -> rule.getSimilarityProperties().stream())
                .collect(Collectors.toList());

        this.similarityTagsProperties = propertiesByName.values().stream()
                .flatMap(List::stream)
                .filter(pd -> pd.similarityTags).collect(Collectors.toList());

        if (defn.hasChildNode(INFERENCE_CONFIG)) {
            this.inferenceDefinition = new InferenceDefinition(defn.getChildNode(INFERENCE_CONFIG));
        } else {
            this.inferenceDefinition = null;
        }
    }

    @Nullable
    public NodeState getAnalyzersNodeState() {
        return definition.getChildNode(FulltextIndexConstants.ANALYZERS);
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    /**
     * Returns the index alias on the Elasticsearch cluster. This alias should be used for any query related operations.
     * The actual index name is used only when a reindex is in progress.
     * @return the Elasticsearch index alias
     */
    public String getIndexAlias() {
        return indexAlias;
    }

    public Map<String, List<PropertyDefinition>> getPropertiesByName() {
        return propertiesByName;
    }

    public List<PropertyDefinition> getDynamicBoostProperties() {
        return dynamicBoostProperties;
    }

    public List<PropertyDefinition> getSimilarityProperties() {
        return similarityProperties;
    }

    public List<PropertyDefinition> getSimilarityTagsProperties() {
        return similarityTagsProperties;
    }

    public String[] getSimilarityTagsFields() {
        return similarityTagsFields;
    }

    public boolean areSimilarityTagsEnabled() {
        return similarityTagsEnabled;
    }

    public float getSimilarityTagsBoost() {
        return similarityTagsBoost;
    }

    /**
     * Returns the keyword field name mapped in Elasticsearch for the specified property name.
     * @param propertyName the property name in the index rules
     * @return the field name identifier in Elasticsearch
     */
    public String getElasticKeyword(String propertyName) {
        List<PropertyDefinition> propertyDefinitions = propertiesByName.get(propertyName);
        if (propertyDefinitions == null) {
            // if there are no property definitions we return the default keyword name
            // this can happen for properties that were not explicitly defined (eg: created with a regex)
            return propertyName + ".keyword";
        }

        String field = propertyName;
        // it's ok to look at the first property since we are sure they all have the same type
        int type = propertyDefinitions.get(0).getType();
        if (isAnalyzable.apply(type) && isAnalyzed(propertyDefinitions)) {
            field += ".keyword";
        }
        return field;
    }

    public boolean isAnalyzed(List<PropertyDefinition> propertyDefinitions) {
        return propertyDefinitions.stream().anyMatch(pd -> pd.analyzed);
    }

    @Override
    protected String getDefaultFunctionName() {
        /*
        This has nothing to do with lucene index. While parsing queries, spellCheck queries are handled
        via PropertyRestriction having native*lucene as key.
         */
        return "lucene";
    }

    /**
     * Returns {@code true} if original terms need to be preserved at indexing analysis phase
     */
    public boolean analyzerConfigIndexOriginalTerms() {
        NodeState analyzersTree = definition.getChildNode(FulltextIndexConstants.ANALYZERS);
        return getOptionalValue(analyzersTree, FulltextIndexConstants.INDEX_ORIGINAL_TERM, false);
    }

    public boolean analyzerConfigSplitOnCaseChange() {
        NodeState analyzersTree = definition.getChildNode(FulltextIndexConstants.ANALYZERS);
        return getOptionalValue(analyzersTree, SPLIT_ON_CASE_CHANGE, false);
    }

    public boolean analyzerConfigSplitOnNumerics() {
        NodeState analyzersTree = definition.getChildNode(FulltextIndexConstants.ANALYZERS);
        return getOptionalValue(analyzersTree, SPLIT_ON_NUMERICS, false);
    }

    /**
     * Returns the mapping version for this index definition.
     * If the version is not specified, the default value is {@code 1.0.0}.
     */
    public String getMappingVersion() {
        return getOptionalValue(definition, PROP_INDEX_MAPPING_VERSION, "1.0.0");
    }

    @Override
    protected PropertyDefinition createPropertyDefinition(IndexDefinition.IndexingRule rule, String name, NodeState nodeState) {
        return new ElasticPropertyDefinition(rule, name, nodeState);
    }

    /**
     * Checks if the ElasticIndexDefinition allows external updates.
     * Definitions including the inference config are considered externally modifiable.
     *
     * @return true if the definition allows external updates, false otherwise
     */
    public boolean isExternallyModifiable() {
        return this.inferenceDefinition != null;
    }

    /**
     * Class to help with {@link ElasticIndexDefinition} creation.
     * The built object represents the index definition only without the node structure.
     */
    public static class Builder extends IndexDefinition.Builder {

        private final String indexPrefix;

        public Builder(@NotNull String indexPrefix) {
            this.indexPrefix = indexPrefix;
        }

        @Override
        public ElasticIndexDefinition build() {
            return (ElasticIndexDefinition) super.build();
        }

        @Override
        public Builder reindex() {
            super.reindex();
            return this;
        }

        @Override
        protected IndexDefinition createInstance(NodeState indexDefnStateToUse) {
            return new ElasticIndexDefinition(root, indexDefnStateToUse, indexPath, indexPrefix);
        }
    }

    /**
     * Represents the inference configuration for an Elasticsearch index definition.
     * This class holds the properties and queries used for inference.
     */
    public static class InferenceDefinition {

        /**
         * List of properties used for inference.
         */
        public List<Property> properties;

        /**
         * List of queries used for inference.
         */
        public List<Query> queries;

        public InferenceDefinition() { }

        /**
         * Constructs an InferenceDefinition from a given NodeState.
         *
         * @param inferenceNode the NodeState containing the inference configuration
         */
        public InferenceDefinition(NodeState inferenceNode) {
            if (inferenceNode.hasChildNode("properties")) {
                this.properties = CollectionUtils.toStream(inferenceNode.getChildNode("properties").getChildNodeEntries())
                        .map(cne -> new Property(cne.getName(), cne.getNodeState()))
                        .collect(Collectors.toList());
            }
            if (inferenceNode.hasChildNode("queries")) {
                this.queries = CollectionUtils.toStream(inferenceNode.getChildNode("queries").getChildNodeEntries())
                        .map(cne -> new Query(cne.getName(), cne.getNodeState()))
                        .collect(Collectors.toList());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferenceDefinition that = (InferenceDefinition) o;
            return Objects.equals(properties, that.properties) && Objects.equals(queries, that.queries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(properties, queries);
        }

        /**
         * Represents a property used for inference.
         */
        public static class Property {
            /**
             * The name of the property. It will be prefixed with "{@link ElasticIndexDefinition#INFERENCE}." when stored in the index.
             */
            public String name;
            /**
             * The model used for inference. Default is "semantic". This will be used by the inference service to determine the model to use.
             */
            public String model;
            /**
             * The fields used for inference. These fields will be used to generate the vector for the inference.
             */
            public List<String> fields;
            /**
             * The number of dimensions for the vector generated for the inference.
             */
            public int dims;
            /**
             * The similarity function used for the inference. Default is "cosine".
             */
            public String similarity;

            // Jackson requires a no-arg constructor for deserialization.
            @SuppressWarnings("unused")
            protected Property() {
                // Default constructor for Jackson deserialization only.
            }

            /**
             * Constructs a Property from a given NodeState.
             *
             * @param name the name of the property
             * @param inferenceNode the NodeState containing the property configuration
             */
            public Property(String name, NodeState inferenceNode) {
                this.name = ElasticIndexDefinition.INFERENCE + "." + name;
                this.model = getOptionalValue(inferenceNode, "model", "semantic");
                this.fields = Arrays.asList(getOptionalValues(inferenceNode, "fields", Type.STRINGS, String.class, new String[0]));
                this.dims = getOptionalValue(inferenceNode, "dims", 1024);
                this.similarity = getOptionalValue(inferenceNode, "similarity", "cosine");
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Property property = (Property) o;
                return dims == property.dims && Objects.equals(name, property.name) &&
                        Objects.equals(model, property.model) &&
                        Objects.equals(fields, property.fields) &&
                        Objects.equals(similarity, property.similarity);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name, model, fields, dims, similarity);
            }
        }

        /**
         * Represents a query used for inference.
         */
        public static class Query {
            /**
             * The name of the query.
             */
            public String name;
            /**
             * The service URL used for the query.
             */
            public String serviceUrl;
            /**
             * The model used for the query. Default is "semantic". It needs to match with one of the models used for the properties.
             */
            public String model;
            /**
             * The prefix used for the query. If the input string starts with this prefix, the query will be executed. Default is null (no prefix).
             */
            public String prefix;
            /**
             * The minimum number of terms required for the query to be executed. Default is 2.
             */
            public int minTerms;
            /**
             * The number of candidates to be returned by the query. Default is 100.
             */
            public int numCandidates;
            /**
             * The type of the query. Default is "hybrid". Currently not used
             */
            public String type; // this can be hybrid or vector
            /**
             * The similarity threshold used for the query. Default is 0.5.
             */
            public float similarityThreshold;
            /**
             * The timeout for the query in milliseconds. Default is 5000.
             */
            public long timeout;

            // Jackson requires a no-arg constructor for deserialization.
            @SuppressWarnings("unused")
            protected Query() {
                // Default constructor for Jackson deserialization only.
            }

            /**
             * Constructs a Query from a given NodeState.
             *
             * @param name the name of the query
             * @param queryNode the NodeState containing the query configuration
             */
            public Query(String name, NodeState queryNode) {
                this.name = name;
                this.serviceUrl = getOptionalValue(queryNode, "serviceUrl", null);
                this.model = getOptionalValue(queryNode, "model", "semantic");
                this.prefix = getOptionalValue(queryNode, "prefix", null);
                this.minTerms = getOptionalValue(queryNode, "minTerms", 2);
                this.numCandidates = getOptionalValue(queryNode, "numCandidates", 100);
                this.type = getOptionalValue(queryNode, "type", "hybrid");
                this.similarityThreshold = getOptionalValue(queryNode, "similarityThreshold", 0.5f);
                this.timeout = getOptionalValue(queryNode, "timeout", 5000L);
            }

            /**
             * Returns {@code true} if the input string is eligible for this query.
             * @param input the input string
             * @return {@code true} if the input string is eligible for this query
             */
            public boolean isEligibleForInput(String input) {
                return prefix == null || input.startsWith(prefix);
            }

            /**
             * Rewrites the input string by removing the prefix.
             *
             * @param input the input string
             * @return the rewritten input string
             */
            public String rewrite(String input) {
                return prefix == null ? input : input.substring(prefix.length());
            }

            /**
             * Checks if the input string has the minimum number of terms required for this query.
             *
             * @param input the input string
             * @return true if the input string has the minimum number of terms
             */
            public boolean hasMinTerms(String input) {
                return minTerms <= input.split("\\s+").length;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Query query = (Query) o;
                return minTerms == query.minTerms && numCandidates == query.numCandidates &&
                        Objects.equals(name, query.name) &&
                        Objects.equals(serviceUrl, query.serviceUrl) &&
                        Objects.equals(model, query.model) &&
                        Objects.equals(prefix, query.prefix) &&
                        Objects.equals(type, query.type) &&
                        similarityThreshold == query.similarityThreshold &&
                        timeout == query.timeout;
            }

            @Override
            public int hashCode() {
                return Objects.hash(name, serviceUrl, model, prefix, minTerms, numCandidates, type, similarityThreshold, timeout);
            }

            @Override
            public String toString() {
                return "Query{" +
                        "name='" + name + '\'' +
                        ", serviceUrl='" + serviceUrl + '\'' +
                        ", model='" + model + '\'' +
                        ", prefix='" + prefix + '\'' +
                        ", minTerms=" + minTerms +
                        ", numCandidates=" + numCandidates +
                        ", type='" + type + '\'' +
                        ", similarityThreshold=" + similarityThreshold +
                        ", timeout=" + timeout +
                        '}';
            }
        }
    }
}
