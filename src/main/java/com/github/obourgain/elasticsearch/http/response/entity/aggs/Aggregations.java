package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

public class Aggregations {

    private Map<String, Aggregation> parsed = new HashMap<>();
    private final Map<String, XContentBuilder> rawAggs = new HashMap<>();

    public Terms getTerms(final String name) {
        return findOrCreate(name, new Converter<Terms>() {
            @Override
            public Terms convert(XContentParser parser) {
                return Terms.parse(parser, name);
            }
        });
    }

    public Min getMin(final String name) {
        return findOrCreate(name, new Converter<Min>() {
            @Override
            public Min convert(XContentParser parser) {
                return Min.parse(parser, name);
            }
        });
    }

    public Max getMax(final String name) {
        return findOrCreate(name, new Converter<Max>() {
            @Override
            public Max convert(XContentParser parser) {
                return Max.parse(parser, name);
            }
        });
    }

    public Avg getAvg(final String name) {
        return findOrCreate(name, new Converter<Avg>() {
            @Override
            public Avg convert(XContentParser parser) {
                return Avg.parse(parser, name);
            }
        });
    }

    public Sum getSum(final String name) {
        return findOrCreate(name, new Converter<Sum>() {
            @Override
            public Sum convert(XContentParser parser) {
                return Sum.parse(parser, name);
            }
        });
    }

    public Stats getStats(final String name) {
        return findOrCreate(name, new Converter<Stats>() {
            @Override
            public Stats convert(XContentParser parser) {
                return Stats.parse(parser, name);
            }
        });
    }

    public ExtendedStats getExtendedStats(final String name) {
        return findOrCreate(name, new Converter<ExtendedStats>() {
            @Override
            public ExtendedStats convert(XContentParser parser) {
                return ExtendedStats.parse(parser, name);
            }
        });
    }

    public ValueCount getValueCount(final String name) {
        return findOrCreate(name, new Converter<ValueCount>() {
            @Override
            public ValueCount convert(XContentParser parser) {
                return ValueCount.parse(parser, name);
            }
        });
    }

    // TODO percentiles w/ keyed
    // TODO percentilesRank w/ keyed

    public Cardinality getCardinality(final String name) {
        return findOrCreate(name, new Converter<Cardinality>() {
            @Override
            public Cardinality convert(XContentParser parser) {
                return Cardinality.parse(parser, name);
            }
        });
    }

    public GeoBounds getGeoBounds(final String name) {
        return findOrCreate(name, new Converter<GeoBounds>() {
            @Override
            public GeoBounds convert(XContentParser parser) {
                return GeoBounds.parse(parser, name);
            }
        });
    }


    // TODO top hits
    // TODO scripted metric / just make a map ?


    public Global getGlobal(final String name) {
        return findOrCreate(name, new Converter<Global>() {
            @Override
            public Global convert(XContentParser parser) {
                return Global.parse(parser, name);
            }
        });
    }

    public Filter getFilter(final String name) {
        return findOrCreate(name, new Converter<Filter>() {
            @Override
            public Filter convert(XContentParser parser) {
                return Filter.parse(parser, name);
            }
        });
    }

    public Filters getFilters(final String name) {
        return findOrCreate(name, new Converter<Filters>() {
            @Override
            public Filters convert(XContentParser parser) {
                return Filters.parse(parser, name);
            }
        });
    }

    public Missing getMissing(final String name) {
        return findOrCreate(name, new Converter<Missing>() {
            @Override
            public Missing convert(XContentParser parser) {
                return Missing.parse(parser, name);
            }
        });
    }

    public Nested getNested(final String name) {
        return findOrCreate(name, new Converter<Nested>() {
            @Override
            public Nested convert(XContentParser parser) {
                return Nested.parse(parser, name);
            }
        });
    }

    public ReverseNested getReverseNested(final String name) {
        return findOrCreate(name, new Converter<ReverseNested>() {
            @Override
            public ReverseNested convert(XContentParser parser) {
                return ReverseNested.parse(parser, name);
            }
        });
    }

    public Children getChildren(final String name) {
        return findOrCreate(name, new Converter<Children>() {
            @Override
            public Children convert(XContentParser parser) {
                return Children.parse(parser, name);
            }
        });
    }

    public SignificantTerms getSignificantTerms(final String name) {
        return findOrCreate(name, new Converter<SignificantTerms>() {
            @Override
            public SignificantTerms convert(XContentParser parser) {
                return SignificantTerms.parse(parser, name);
            }
        });
    }

    public Range getRange(final String name) {
        return findOrCreate(name, new Converter<Range>() {
            @Override
            public Range convert(XContentParser parser) {
                return Range.parse(parser, name);
            }
        });
    }

    public DateRange getDateRange(final String name) {
        return findOrCreate(name, new Converter<DateRange>() {
            @Override
            public DateRange convert(XContentParser parser) {
                return DateRange.parse(parser, name);
            }
        });
    }

    public IPV4Range getIPV4Range(final String name) {
        return findOrCreate(name, new Converter<IPV4Range>() {
            @Override
            public IPV4Range convert(XContentParser parser) {
                return IPV4Range.parse(parser, name);
            }
        });
    }

    public Histogram getHistogram(final String name) {
        return findOrCreate(name, new Converter<Histogram>() {
            @Override
            public Histogram convert(XContentParser parser) {
                return Histogram.parse(parser, name);
            }
        });
    }

    public DateHistogram getDateHistogram(final String name) {
        return findOrCreate(name, new Converter<DateHistogram>() {
            @Override
            public DateHistogram convert(XContentParser parser) {
                return DateHistogram.parse(parser, name);
            }
        });
    }

    // TODO geodistance
    // TODO geohash grid

    private <T extends Aggregation> T findOrCreate(String name, Converter<T> converter) {
        T t = (T) parsed.get(name);
        if (t != null) {
            return t;
        } else {
            XContentBuilder builder = rawAggs.get(name);
            try {
                if (builder != null) {
                    try (XContentParser parser = XContentHelper.createParser(builder.bytes())) {
                        t = converter.convert(parser);
                        parsed.put(name, t);
                        return t;
                    }
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void addRawAgg(String name, XContentBuilder rawAgg) {
        rawAggs.put(name, rawAgg);
    }

    /*
    parses something like : (with the heading parenthesis)

    {
        "grades_stats": {
        "count": 6,
                "min": 60,
                "max": 98,
                "avg": 78.5,
                "sum": 471
        }
    }

     */
    public static Aggregations parse(XContentParser parser) {
        try {
            assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            Aggregations aggregations = new Aggregations();
            Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == START_OBJECT) {
                    XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                    aggregations.addRawAgg(currentFieldName, docBuilder.copyCurrentStructure(parser));
                    docBuilder.close();
                }
            }
            return aggregations;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    parses something like (e.g. for sub aggs)

    "avg_price" : {
      "value" : 56.3
    }

     */
    protected static Pair<String, XContentBuilder> parseInnerAgg(XContentParser parser, String aggregationName) {
        try {
            assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            docBuilder.copyCurrentStructure(parser);
            docBuilder.close();
            return Pair.of(aggregationName, docBuilder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private interface Converter<T> {
        T convert(XContentParser parser);
    }

    @Override
    public String toString() {
        return "Aggregations{" +
                "knownAggregations=" + rawAggs.keySet() +
                '}';
    }
}
