package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class TermsTest {

    @Test
    public void should_parse_bucket() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/terms/bucket.json");

        Terms.Bucket bucket = Terms.parseBucket(XContentHelper.createParser(new BytesArray(json)));

        assertThat(bucket.getKey()).isEqualTo("2");
        assertThat(bucket.getDocCount()).isEqualTo(1);
    }

    @Test
    public void should_parse_buckets() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/terms/buckets.json");

        List<Terms.Bucket> buckets = Terms.parseBuckets(XContentHelper.createParser(new BytesArray(json)));

        assertThat(buckets)
                .hasSize(2)
                .containsExactly(
                        new Terms.Bucket(0, "message", 1, null),
                        new Terms.Bucket(0, "the", 1, null));
    }

    @Test
    public void should_parse_terms() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/terms/terms.json");

        Terms terms = Terms.parse(XContentHelper.createParser(new BytesArray(json)), "foo");

        assertThat(terms.getName()).isEqualTo("foo");

        assertThat(terms.getDocCountErrorUpperBound()).isEqualTo(3);
        assertThat(terms.getSumOtherDocCount()).isEqualTo(2);
        assertThat(terms.getBuckets())
                .hasSize(2)
                .containsExactly(
                        new Terms.Bucket(0, "message", 1, null),
                        new Terms.Bucket(0, "the", 1, null));
    }

    @Test
    public void should_parse_terms_with_sub_agg() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/terms/terms_with_sub_agg.json");

        Terms terms = Terms.parse(XContentHelper.createParser(new BytesArray(json)), "foo");

        assertThat(terms.getName()).isEqualTo("foo");

        assertThat(terms.getDocCountErrorUpperBound()).isEqualTo(3);
        assertThat(terms.getSumOtherDocCount()).isEqualTo(2);

        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket.getDocCount()).isEqualTo(1);
            assertThat(bucket.getDocCountErrorUpperBound()).isEqualTo(0);
            assertThat(bucket.getKey()).isIn("the", "message");

            assertThat(bucket.getAggregations()).isNotNull();
            assertThat(bucket.getAggregations().getCardinality("author_count")).isNotNull();
            assertThat(bucket.getAggregations().getCardinality("author_count").getValue()).isEqualTo(1);
        }
    }
}