package org.elasticsearch.action.admin.indices.template.put;

import java.util.Map;
import java.util.Set;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

/**
 * @author olivier bourgain
 */
public class PutIndexTemplateRequestAccessor {

    public static Set<Alias> aliases(PutIndexTemplateRequest request) {
        return request.aliases();
    }

    public static Map<String, IndexMetaData.Custom> customs(PutIndexTemplateRequest request) {
        return request.customs();
    }

    public static Settings settings(PutIndexTemplateRequest request) {
        return request.settings();
    }

    public static Map<String, String> mappings(PutIndexTemplateRequest request) {
        return request.mappings();
    }
}
