package com.github.obourgain.elasticsearch.http.request;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Strings;

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

    public RequestUriBuilder addQueryParameterIfNotNull(String name, String value) {
        if(value != null) {
            addQueryParameter(name, value);
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

    public RequestUriBuilder addConsistency(ShardReplicationOperationRequest request) {
        switch (request.consistencyLevel()) {
            case DEFAULT:
                // noop
                break;
            case ALL:
            case QUORUM:
            case ONE:
                addQueryParameter("consistency", request.consistencyLevel().name().toLowerCase());
                break;
            default:
                throw new IllegalStateException("consistency  " + request.consistencyLevel() + " is not supported");
        }
        switch (request.replicationType()) {
            case DEFAULT:
                // noop
                break;
            case SYNC:
            case ASYNC:
                addQueryParameter("replication", request.replicationType().name().toLowerCase());
                break;
            default:
                throw new IllegalStateException("replication  " + request.replicationType() + " is not supported");
        }
        return this;
    }

    @Override
    public String toString() {
        return builder.toString();
    }
}
