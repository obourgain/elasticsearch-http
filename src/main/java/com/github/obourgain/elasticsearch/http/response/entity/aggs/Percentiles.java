package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;

public class Percentiles extends AbtractAggregation {

    private List<Percentile> percentiles;

    public Percentiles(String name) {
        super(name);
    }

    public List<Percentile> getPercentiles() {
        return percentiles;
    }

    public static Percentiles parse(XContentParser parser, String name) {
        try {
            Percentiles range = new Percentiles(name);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && "values".equals(currentFieldName)) {
                    // keyed
                    range.percentiles = parseKeyedPercentiles(parser);
                } else if (token == XContentParser.Token.START_ARRAY && "values".equals(currentFieldName)) {
                    // not keyed
                    range.percentiles = parsePercentiles(parser);
                }
            }
            return range;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<Percentile> parsePercentiles(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
        XContentParser.Token token;
        List<Percentile> result = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                result.add(parsePercentile(parser));
            }
        }
        return result;
    }

    protected static Percentile parsePercentile(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
        XContentParser.Token token;
        String currentFieldName = null;
        Percentile percentile = new Percentile();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("key".equals(currentFieldName)) {
                    percentile.key = parser.doubleValue();
                } else if ("value".equals(currentFieldName)) {
                    percentile.value = parser.doubleValue();
                }
            }
        }
        return percentile;
    }

    protected static List<Percentile> parseKeyedPercentiles(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
        XContentParser.Token token;
        List<Percentile> result = new ArrayList<>();
        Percentile currentPercentile = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentPercentile = new Percentile();
                currentPercentile.key = Double.parseDouble(parser.text());
            } else if (token.isValue()) {
                assert currentPercentile != null;
                currentPercentile.value = parser.doubleValue();
                result.add(currentPercentile);
            }
        }
        return result;
    }

}
