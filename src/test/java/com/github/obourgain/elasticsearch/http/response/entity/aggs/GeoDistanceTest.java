package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class GeoDistanceTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/geodistance/geodistance.json");

        GeoDistance geo = GeoDistance.parse(XContentHelper.createParser(new BytesArray(json)), "geo");

        assertThat(geo.getName()).isEqualTo("geo");

        List<GeoDistance.Bucket> buckets = geo.getBuckets();
        assertThat(buckets).hasSize(3);

        GeoDistance.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo("*-100.0");
        assertThat(bucket.getFrom()).isEqualTo(0);
        assertThat(bucket.getTo()).isEqualTo(100);
        assertThat(bucket.getDocCount()).isEqualTo(17);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isEqualTo("100.0-300.0");
        assertThat(bucket.getFrom()).isEqualTo(100);
        assertThat(bucket.getTo()).isEqualTo(300);
        assertThat(bucket.getDocCount()).isEqualTo(5);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(2);
        assertThat(bucket.getKey()).isEqualTo("300.0-*");
        assertThat(bucket.getFrom()).isEqualTo(300);
        assertThat(bucket.getTo()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(12);
        assertThat(bucket.getAggregations()).isNull();
    }

}