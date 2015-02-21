package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;
import com.github.obourgain.elasticsearch.http.response.entity.Hit;
import com.github.obourgain.elasticsearch.http.response.entity.Hits;

public class TopHitsTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/tophits/tophits.json");

        TopHits topHits = TopHits.parse(XContentHelper.createParser(new BytesArray(json)), "top_tags_hits");

        assertThat(topHits.getName()).isEqualTo("top_tags_hits");

        assertThat(topHits.getHits()).hasSize(1);

        Hit hit = topHits.getHits().getAt(0);
        assertThat(hit.getSort()).hasSize(1);
        assertThat(hit.getSort().get(0)).isEqualTo("1370143231177");

        assertThat(hit.getIndex()).isEqualTo("stack");
        assertThat(hit.getType()).isEqualTo("question");
        assertThat(hit.getId()).isEqualTo("602679");
        assertThat(hit.getScore()).isEqualTo(1);
        assertThat(hit.getSource().length).isGreaterThan(0);
    }

    @Test
    public void should_parse_as_nested() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/tophits/tophits_as_nested.json");

        Terms terms = Terms.parse(XContentHelper.createParser(new BytesArray(json)), "top_tags");

        List<Terms.Bucket> buckets = terms.getBuckets();

        assertThat(buckets).hasSize(3);

        Terms.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo("windows-7");

        TopHits topHits = bucket.getAggregations().getTopHits("top_tags_hits");
        assertThat(topHits).isNotNull();
        assertThat(topHits.getName()).isEqualTo("top_tags_hits");
        Hits hits = topHits.getHits();
        assertThat(hits.getTotal()).isEqualTo(25365);
        assertThat(hits.getMaxScore()).isEqualTo(1);
        assertThat(hits.getHits()).hasSize(1);
        assertThat(hits.getHits().get(0).getId()).isEqualTo("602679");
    }
}