package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class GeoBounds extends AbstractAggregation {
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
                        Coordinates coordinates = coordinates(parser);
                        geoBounds.topLeftLat = coordinates.lat;
                        geoBounds.topLeftLon = coordinates.lon;
                    } else if ("bottom_right".equals(currentFieldName)) {
                        Coordinates coordinates = coordinates(parser);
                        geoBounds.bottomRightLat = coordinates.lat;
                        geoBounds.bottomRightLon = coordinates.lon;
                    }
                }
            }
            return geoBounds;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Coordinates coordinates(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Coordinates coordinates = new Coordinates();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("lat".equals(currentFieldName)) {
                    coordinates.lat = parser.doubleValue();
                } else if ("lon".equals(currentFieldName)) {
                    coordinates.lon = parser.doubleValue();
                }
            }
        }
        return coordinates;
    }

    private static class Coordinates {
        double lat;
        double lon;
    }

}
