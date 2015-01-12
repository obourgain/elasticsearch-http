package com.github.obourgain.elasticsearch.http.response;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geohashGrid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import java.io.IOException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.junit.Test;

public class AggregationParserTest {

    @Test
    public void should_find_aggs() throws IOException {
        TermsBuilder foo = terms("foo");
        Client client = new TransportClient();
        SearchRequest request = client.prepareSearch("").addAggregation(foo).request();

        AggregationParser.parseQuery(request);

    }

    @Test
    public void should_find_sub_aggs() throws IOException {
        TermsBuilder foo = terms("foo")
                .subAggregation(avg("bar"))
                .subAggregation(min("baz"));
        Client client = new TransportClient();
        SearchRequest request = client.prepareSearch("").addAggregation(foo).request();

        String s = XContentHelper.convertToJson(new BytesArray(request.source().toBytes()), true);
        System.out.println(s);
        System.out.println(AggregationParser.parseQuery(request));

    }

    @Test
    public void should_find_sub_aggs_for_any_nesting_level() throws IOException {
        TermsBuilder foo = terms("foo")
                .subAggregation(geohashGrid("bar")
                        .subAggregation(min("baz")));
        Client client = new TransportClient();
        SearchRequest request = client.prepareSearch("").addAggregation(foo).request();

        String s = XContentHelper.convertToJson(new BytesArray(request.source().toBytes()), true);
        System.out.println(s);
        System.out.println(AggregationParser.parseQuery(request));
    }

    @Test
    public void should_find_several_root_aggs() throws IOException {
        TermsBuilder foo = terms("foo");
        GeoHashGridBuilder bar = geohashGrid("bar");
        Client client = new TransportClient();
        SearchRequest request = client.prepareSearch("")
                .addAggregation(foo)
                .addAggregation(bar)
                .request();

        String s = XContentHelper.convertToJson(new BytesArray(request.source().toBytes()), true);
        System.out.println(s);
        System.out.println(AggregationParser.parseQuery(request));
    }

}