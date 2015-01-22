package com.github.obourgain.elasticsearch.http.response;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author olivier bourgain
 */
public class EntityWrapper implements Map<String, Object> {

    private static final Logger logger = LoggerFactory.getLogger(EntityWrapper.class);
//    private static final ObjectMapper mapper = new ObjectMapper();

    private String entityAsString;
    private Map<String, Object> entityAsMap;

    public EntityWrapper(String responseEntity) {
        this.entityAsString = responseEntity;
    }

    private Map<String, Object> jsonToMap() {
//        if (entityAsMap == null) {
//            try {
//                entityAsMap = mapper.readValue(entityAsString, new TypeReference<HashMap<String, Object>>() {
//                });
//                logger.trace("deserialized {}", entityAsMap);
//            } catch (IOException e) {
//                logger.info("Unable to deserialize {}", entityAsString);
//                throw new RuntimeException(e);
//            }
//        }
        return entityAsMap;
    }

    @Override
    public int size() {
        return jsonToMap().size();
    }

    @Override
    public boolean isEmpty() {
        return jsonToMap().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return jsonToMap().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return jsonToMap().containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return jsonToMap().get(key);
    }

    @Override
    public Object put(String key, Object value) {
        throw new IllegalStateException("not supported");
    }

    @Override
    public Object remove(Object key) {
        throw new IllegalStateException("not supported");
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        throw new IllegalStateException("not supported");
    }

    @Override
    public void clear() {
        throw new IllegalStateException("not supported");
    }

    @Override
    public Set<String> keySet() {
        return jsonToMap().keySet();
    }

    @Override
    public Collection<Object> values() {
        return jsonToMap().values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return jsonToMap().entrySet();
    }
}
