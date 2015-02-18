package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class GeoBounds extends AbtractAggregation {
    private double topLeftLat;
    private double topLeftLon;
    private double bottomRightLat;
    private double bottomRightLon;

    public GeoBounds(String name) {
        super(name);
    }

    public static GeoBounds parse(XContentParser parser, String name) {
        try {
            GeoBounds geoBounds = new GeoBounds(name);

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("top_left".equals(currentFieldName)) {
                        parseTopLeft(parser, geoBounds);
                    } else if ("bottom_right".equals(currentFieldName)) {
                        parseBottomRight(parser, geoBounds);
                    }
                }
            }
            return geoBounds;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void parseTopLeft(XContentParser parser, GeoBounds geoBounds) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("lat".equals(currentFieldName)) {
                    geoBounds.topLeftLat = parser.doubleValue();
                } else if ("lon".equals(currentFieldName)) {
                    geoBounds.topLeftLon = parser.doubleValue();
                }
            }
        }
    }

    private static void parseBottomRight(XContentParser parser, GeoBounds geoBounds) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("lat".equals(currentFieldName)) {
                    geoBounds.bottomRightLat = parser.doubleValue();
                } else if ("lon".equals(currentFieldName)) {
                    geoBounds.bottomRightLon = parser.doubleValue();
                }
            }
        }
    }

}
