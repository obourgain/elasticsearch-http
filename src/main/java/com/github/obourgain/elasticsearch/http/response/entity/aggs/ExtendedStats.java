package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class ExtendedStats extends Stats {

    private double sumOfSqrs;
    private double variance;
    private double stdDev;
    private StdDevBounds stdDevBounds;

    public ExtendedStats(String name) {
        super(name);
    }

    public static ExtendedStats parse(XContentParser parser, String name) {
        try {
            ExtendedStats stats = new ExtendedStats(name);
            StdDevBounds bounds = new StdDevBounds();
            stats.stdDevBounds = bounds;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("count".equals(currentFieldName)) {
                        stats.count = parser.longValue();
                    } else if ("min".equals(currentFieldName)) {
                        stats.min = parser.doubleValue();
                    } else if ("max".equals(currentFieldName)) {
                        stats.max = parser.doubleValue();
                    } else if ("avg".equals(currentFieldName)) {
                        stats.avg = parser.doubleValue();
                    } else if ("sum".equals(currentFieldName)) {
                        stats.sum = parser.doubleValue();
                    } else if ("sum_of_squares".equals(currentFieldName)) {
                        stats.sumOfSqrs = parser.doubleValue();
                    } else if ("variance".equals(currentFieldName)) {
                        stats.variance = parser.doubleValue();
                    } else if ("std_deviation".equals(currentFieldName)) {
                        stats.stdDev = parser.doubleValue();
                    } else if ("upper".equals(currentFieldName)) {
                        bounds.upper = parser.doubleValue();
                    } else if ("lower".equals(currentFieldName)) {
                        bounds.lower = parser.doubleValue();
                    }
                }
            }
            return stats;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    public static class StdDevBounds {
        private double upper;
        private double lower;
    }
}
