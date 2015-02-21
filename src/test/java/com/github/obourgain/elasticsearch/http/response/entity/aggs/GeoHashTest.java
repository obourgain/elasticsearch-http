package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class GeoHashTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/geohash/geohash.json");

        GeoHash geoHash = GeoHash.parse(XContentHelper.createParser(new BytesArray(json)), "geo");

        assertThat(geoHash.getName()).isEqualTo("geo");

        List<GeoHash.Bucket> buckets = geoHash.getBuckets();
        assertThat(buckets).hasSize(2);

        GeoHash.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo("svz");
        assertThat(bucket.getDocCount()).isEqualTo(10964);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isEqualTo("sv8");
        assertThat(bucket.getDocCount()).isEqualTo(3198);
        assertThat(bucket.getAggregations()).isNull();
    }
}