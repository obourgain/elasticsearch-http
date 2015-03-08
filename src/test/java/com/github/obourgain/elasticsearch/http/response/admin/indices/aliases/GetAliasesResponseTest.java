package com.github.obourgain.elasticsearch.http.response.admin.indices.aliases;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class GetAliasesResponseTest {

    @Test
    public void should_parse_empty_response() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/aliases/empty_response.json");

        GetAliasesResponse aliasesResponse = new GetAliasesResponse().doParse(new BytesArray(json));

        assertThat(aliasesResponse.getAliases()).hasSize(0);
    }

    @Test
    public void should_parse_response() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/aliases/response.json");

        GetAliasesResponse aliasesResponse = new GetAliasesResponse().doParse(new BytesArray(json));

        ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesResponse.getAliases();

        assertThat(aliases).hasSize(2);

        assertThat(aliases.containsKey("users1")).isTrue();
        List<AliasMetaData> alias1 = aliases.get("users1");
        assertThat(alias1).hasSize(1);

        assertThat(aliases.containsKey("users2")).isTrue();
        List<AliasMetaData> alias2 = aliases.get("users2");
        assertThat(alias2).hasSize(1);
    }

    @Test
    public void should_parse_aliases() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/aliases/index_alias_infos.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        List<AliasMetaData> metaDatas = GetAliasesResponse.parseAliases(parser);

        assertThat(metaDatas).hasSize(1);
        AliasMetaData metaData = metaDatas.get(0);

        assertThat(metaData.filteringRequired()).isTrue();
        assertThat(metaData.indexRouting()).isEqualTo("foo");
        assertThat(metaData.searchRouting()).isEqualTo("foo");
        assertThat(metaData.filter().string()).contains("term");
        assertThat(metaData.filter().string()).contains("key");
        assertThat(metaData.filter().string()).contains("value");
    }

    @Test
    public void should_parse_aliases_2() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/aliases/index_alias_infos_2.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        List<AliasMetaData> metaDatas = GetAliasesResponse.parseAliases(parser);

        assertThat(metaDatas).hasSize(2);

        {
            AliasMetaData metaData = metaDatas.get(0);
            assertThat(metaData.filteringRequired()).isTrue();
            assertThat(metaData.indexRouting()).isEqualTo("foo");
            assertThat(metaData.searchRouting()).isEqualTo("foo");
            assertThat(metaData.filter().string()).contains("term");
            assertThat(metaData.filter().string()).contains("key");
            assertThat(metaData.filter().string()).contains("value");
        }
        {
            AliasMetaData metaData = metaDatas.get(1);
            assertThat(metaData.filteringRequired()).isTrue();
            assertThat(metaData.indexRouting()).isEqualTo("bar");
            assertThat(metaData.searchRouting()).isEqualTo("bar");
            assertThat(metaData.filter().string()).contains("term");
            assertThat(metaData.filter().string()).contains("key");
            assertThat(metaData.filter().string()).contains("value2");
        }

    }

    @Test
    public void should_parse_alias() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/aliases/alias_infos.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        // skip start object and field name
        parser.nextToken();
        parser.nextToken();

        AliasMetaData metaData = GetAliasesResponse.parseAlias(parser);
        assertThat(metaData.filteringRequired()).isTrue();
        assertThat(metaData.indexRouting()).isEqualTo("foo");
        assertThat(metaData.searchRouting()).isEqualTo("foo");
        assertThat(metaData.filter().string()).contains("term");
        assertThat(metaData.filter().string()).contains("key");
        assertThat(metaData.filter().string()).contains("value");
    }
}