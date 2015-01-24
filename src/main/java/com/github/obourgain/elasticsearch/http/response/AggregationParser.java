package com.github.obourgain.elasticsearch.http.response;

import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

public class AggregationParser {

    public static Map<String, AggregationMetaInfos> parseQuery(SearchRequest request) {
        try {
            XContentParser parser = XContentHelper.createParser(request.source());

            XContentParser.Token firstToken = parser.nextToken(); // first start object
            assert firstToken == XContentParser.Token.START_OBJECT;

            XContentParser.Token token;
            XContentParser.Token nextToken;
            while ((token = parser.nextToken()) != null) {
                switch (token) {
                    case START_ARRAY:
                    case START_OBJECT:
                        parser.skipChildren();
                        break;
                    case END_ARRAY:
                    case END_OBJECT:
                        break;
                    case FIELD_NAME:
                        String text = parser.text();
                        if (text.equals("aggs") || text.equals("aggregations")) {
                            nextToken = parser.nextToken();
                            assert nextToken == START_OBJECT;
                            return parseAggregators(parser, 0);
                        }
                        break;
                    case VALUE_BOOLEAN:
                    case VALUE_NULL:
                    case VALUE_NUMBER:
                    case VALUE_STRING:
                    case VALUE_EMBEDDED_OBJECT:
                        break;
                    default:
                        throw new IllegalStateException("token type " + token + " is not supported");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private static Map<String, AggregationMetaInfos> parseAggregators(XContentParser parser, int level) throws IOException {
        // taken from org.elasticsearch.search.aggregations.AggregatorParsers.parseAggregators()
        Map<String, AggregationMetaInfos> result = new HashMap<>();

        XContentParser.Token token;
        // don't validate the agg name as it was already done at server side. If we get there, ES parsed correctly the query.

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation.");
            }
            final String aggregationName = parser.currentName();
            XContentBuilder aggConfig = null;
            String aggregationType = null;

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("Aggregation definition for [" + aggregationName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
            }

            Map<String, AggregationMetaInfos> subAggs = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalStateException("Expected [" + XContentParser.Token.FIELD_NAME + "] under a [" + XContentParser.Token.START_OBJECT + "], but got a [" + token + "] in [" + aggregationName + "]");
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalStateException("Expected [" + XContentParser.Token.START_OBJECT + "] under [" + fieldName + "], but got a [" + token + "] in [" + aggregationName + "]");
                }

                switch (fieldName) {
                    case "aggregations":
                    case "aggs":
                        if (subAggs != null) {
                            throw new IllegalStateException("Found two sub aggregation definitions under [" + aggregationName + "]");
                        }
                        subAggs = parseAggregators(parser, level + 1);
                        break;
                    default:
                        aggregationType = fieldName;
                        aggConfig = XContentFactory.jsonBuilder();
                        XContentHelper.copyCurrentStructure(aggConfig.generator(), parser);
                        aggConfig.close();
                }
            }
            AggregationMetaInfos aggregationMetaInfos = new AggregationMetaInfos(aggregationName, aggregationType, subAggs, aggConfig);
            result.put(aggregationName, aggregationMetaInfos);
        }
        return result;
    }

}
