package com.github.obourgain.elasticsearch.http.request;

import java.util.Collection;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.VersionType;

public class RequestUriBuilder {

    private StringBuilder builder = new StringBuilder();
    private boolean addedQueryParamSeparator = false;

    public RequestUriBuilder() {
    }

    public RequestUriBuilder(String index) {
        addWithPathSeparatorBefore(index);
    }

    public RequestUriBuilder(String index, String type) {
        this(index);
        addWithPathSeparatorBefore(type);
    }

    public RequestUriBuilder(String index, String type, String id) {
        this(index, type);
        addWithPathSeparatorBefore(id);
    }

    public RequestUriBuilder addEndpoint(String endpoint) {
        assert !addedQueryParamSeparator;
        addWithPathSeparatorBefore(endpoint);
        return this;
    }

    public RequestUriBuilder addQueryParameter(String name, String value) {
        addQueryStringSeparator();
        builder.append(name);
        builder.append("=");
        builder.append(value);
        return this;
    }

    public RequestUriBuilder addQueryParameterArrayAsCommaDelimited(String name, String ... values) {
        addQueryStringSeparator();
        builder.append(name);
        builder.append("=");
        Strings.arrayToDelimitedString(values, ",", builder);
        return this;
    }

    public RequestUriBuilder addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty(String name, String ... values) {
        if(values != null && values.length != 0) {
            addQueryStringSeparator();
            builder.append(name);
            builder.append("=");
            Strings.arrayToDelimitedString(values, ",", builder);
        }
        return this;
    }

    public RequestUriBuilder addQueryParameterCollectionAsCommaDelimitedIfNotNullNorEmpty(String name, Collection<String> values) {
        if(values != null && values.size() != 0) {
            addQueryStringSeparator();
            builder.append(name);
            builder.append("=");
            Strings.collectionToDelimitedString(values, ",", "", "", builder);
        }
        return this;
    }

    public RequestUriBuilder addQueryParameter(String name, boolean value) {
        addQueryStringSeparator();
        builder.append(name);
        builder.append("=");
        builder.append(value);
        return this;
    }

    public RequestUriBuilder addQueryParameter(String name, long value) {
        addQueryStringSeparator();
        builder.append(name);
        builder.append("=");
        builder.append(value);
        return this;
    }

    public RequestUriBuilder addQueryParameterIfNotMinusOne(String name, long value) {
        if(value != -1) {
            addQueryStringSeparator();
            builder.append(name);
            builder.append("=");
            builder.append(value);
        }
        return this;
    }

    public RequestUriBuilder addQueryParameterIfNotZero(String name, long value) {
        if(value != 0) {
            addQueryStringSeparator();
            builder.append(name);
            builder.append("=");
            builder.append(value);
        }
        return this;
    }

    public RequestUriBuilder addQueryParameterIfNotNull(String name, String value) {
        if(value != null) {
            addQueryParameter(name, value);
        }
        return this;
    }

    public RequestUriBuilder addQueryParameterIfNotNull(String name, Boolean value) {
        if(value != null) {
            addQueryParameter(name, value);
        }
        return this;
    }

    public RequestUriBuilder addSearchType(SearchType searchType) {
        switch (searchType) {
            case COUNT:
            case QUERY_AND_FETCH:
            case QUERY_THEN_FETCH:
            case DFS_QUERY_AND_FETCH:
            case DFS_QUERY_THEN_FETCH:
            case SCAN:
                addQueryParameter("search_type", searchType.name().toLowerCase());
                break;
            default:
                throw new IllegalStateException("search_type " + searchType + " is not supported");
        }
        return this;
    }
    
    public RequestUriBuilder addVersionType(VersionType versionType) {
        switch (versionType) {
            case EXTERNAL:
            case EXTERNAL_GTE:
            case FORCE:
                addQueryParameter("version_type", versionType.name().toLowerCase());
                break;
            case INTERNAL:
                // noop
                break;
            default:
                throw new IllegalStateException("version_type " + versionType + " is not supported");
        }
        return this;
    }

    public RequestUriBuilder addConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        switch (consistencyLevel) {
            case DEFAULT:
                // noop
                break;
            case ALL:
            case QUORUM:
            case ONE:
                addQueryParameter("consistency", consistencyLevel.name().toLowerCase());
                break;
            default:
                throw new IllegalStateException("consistency  " + consistencyLevel + " is not supported");        }
        return this;
    }

    public RequestUriBuilder addReplicationType(ReplicationType replicationType) {
        switch (replicationType) {
            case DEFAULT:
                // noop
                break;
            case SYNC:
            case ASYNC:
                addQueryParameter("replication", replicationType.name().toLowerCase());
                break;
            default:
                throw new IllegalStateException("replication  " + replicationType + " is not supported");
        }
        return this;
    }

    public void addIndicesOptions(IndicesRequest request) {
        IndicesOptions indicesOptions = request.indicesOptions();
        addQueryParameter("ignore_unavailable", indicesOptions.ignoreUnavailable());
        addQueryParameter("allow_no_indices", indicesOptions.allowNoIndices());

        // TODO how are those set ?
        indicesOptions.allowAliasesToMultipleIndices();
        indicesOptions.forbidClosedIndices();
    }

    public RequestUriBuilder addQueryParameter(String name, String ... values) {
        addQueryStringSeparator();
        for (String value : values) {
            builder.append(name);
            builder.append("=");
            builder.append(value);
        }
        return this;
    }

    private void addWithPathSeparatorBefore(String toAdd) {
        builder.append("/").append(toAdd);
    }

    private void addQueryStringSeparator() {
        if(addedQueryParamSeparator) {
            builder.append("&");
        } else {
            builder.append("?");
            addedQueryParamSeparator = true;
        }
    }

    @Override
    public String toString() {
        return builder.toString();
    }
}
