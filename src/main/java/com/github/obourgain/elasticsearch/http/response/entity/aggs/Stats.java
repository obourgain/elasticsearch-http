package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Stats extends AbtractAggregation {

    protected long count;
    protected double min;
    protected double max;
    protected double avg;
    protected double sum;

    public Stats(String name) {
        super(name);
    }

    public static Stats parse(XContentParser parser, String name) {
        try {
            Stats stats = new Stats(name);

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
                    }
                }
            }
            return stats;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
