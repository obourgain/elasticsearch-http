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

    /**
     * This exposes the package visible aliases() method {@link PutIndexTemplateRequest#aliases()}.
     */
    public static Set<Alias> aliases(PutIndexTemplateRequest request) {
        return request.aliases();
    }

    /**
     * This exposes the package visible customs() method {@link PutIndexTemplateRequest#customs()}.
     */
    public static Map<String, IndexMetaData.Custom> customs(PutIndexTemplateRequest request) {
        return request.customs();
    }

    /**
     * This exposes the package visible settings() method {@link PutIndexTemplateRequest#settings()}.
     */
    public static Settings settings(PutIndexTemplateRequest request) {
        return request.settings();
    }

    public static Map<String, String> mappings(PutIndexTemplateRequest request) {
        return request.mappings();
    }
}
