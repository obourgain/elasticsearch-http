package org.elasticsearch.action.admin.indices.create;

import java.util.Map;
import java.util.Set;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @author olivier bourgain
 */
public class CreateIndexRequestAccessor {

    public static String index(CreateIndexRequest request) {
        return request.index();
    }

    public static String cause(CreateIndexRequest request) {
        return request.cause();
    }

    public static Settings settings(CreateIndexRequest request) {
        return request.settings();
    }

    public static Map<String, String> mappings(CreateIndexRequest request) {
        return request.mappings();
    }

    public static Map<String, IndexMetaData.Custom> customs(CreateIndexRequest request) {
        return request.customs();
    }

    public static TimeValue timeout(CreateIndexRequest request) {
        return request.timeout();
    }

    public static TimeValue masterNodeTimeout(CreateIndexRequest request) {
        return request.masterNodeTimeout();
    }

    public static Set<Alias> aliases(CreateIndexRequest request) {
        return request.aliases();
    }
}
