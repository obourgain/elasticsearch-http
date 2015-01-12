package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * @author olivier bourgain
 */
public class UpdateHelper {

    public static String buildRequestBody(UpdateRequest request) throws IOException {
        // if/else should not close the object
        XContentBuilder requestBody = XContentFactory.jsonBuilder().startObject();
        if (request.doc() != null && request.script() == null) {
            // TODO this throws a DocumentMissingException in the rest action but a test is expecting an ActionRequestValidationException
            Tuple<XContentType, Map<String, Object>> docAsString = XContentHelper.convertToMap(request.doc().source(), false);
            requestBody = XContentFactory.jsonBuilder().startObject().field("doc").map(docAsString.v2());
        }
        if (request.upsertRequest() != null) {
            // TODO lots of options on upsertRequest
            Map<String, Object> upsertAsMap = XContentHelper.convertToMap(request.upsertRequest().source(), false).v2();
            requestBody.field("upsert", upsertAsMap);
            if (request.script() != null) {
                requestBody.field("script", request.script());
            }
            addScriptParams(request, requestBody);
            if (request.scriptedUpsert()) {
                requestBody.field("scripted_upsert", request.scriptedUpsert());
            }
        } else if (request.docAsUpsert()) {
            // request.doc() may be null if there is only a script
            requestBody.field("doc_as_upsert", true);
            if (request.doc() != null) {
                Map<String, Object> docAsMap = XContentHelper.convertToMap(request.doc().source(), false).v2();
                requestBody.field("doc", docAsMap);
            }
            if (request.script() != null) {
                requestBody.field("script", request.script());
            }
            addScriptParams(request, requestBody);
        } else if (request.script() != null) {
            requestBody.field("script", request.script());
            addScriptParams(request, requestBody);
        }

        if(request.detectNoop()) {
           requestBody.field("detect_noop", String.valueOf(request.detectNoop()));
        }
        return requestBody.endObject().string();
    }

    private static void addScriptParams(UpdateRequest request, XContentBuilder builder) throws IOException {
        if (request.scriptParams() != null) {
            builder.startObject("params");
            for (Map.Entry<String, Object> entry : request.scriptParams().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
    }

}
