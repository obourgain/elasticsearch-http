package com.github.obourgain.elasticsearch.http.response.document.bulk;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class BulkItemTest {

    /*
    samples taken from ES documentation

    curl -s -XPOST localhost:9200/_bulk --data-binary @requests; echo

    with requests :
    { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
    { "field1" : "value1" }
    { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
    { "create" : { "_index" : "test", "_type" : "type1", "_id" : "3" } }
    { "field1" : "value3" }
    { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
    { "doc" : {"field2" : "value2"} }
     */

    @Test
    public void should_parse_index() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/document/bulk/bulk_item_index.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        BulkItem bulkItem = BulkItem.parse(parser);

        assertIndex(bulkItem);
    }

    @Test
    public void should_parse_create() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/document/bulk/bulk_item_create.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        BulkItem bulkItem = BulkItem.parse(parser);

        assertCreate(bulkItem);
    }

    @Test
    public void should_parse_update() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/document/bulk/bulk_item_update.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        BulkItem bulkItem = BulkItem.parse(parser);

        assertUpdate(bulkItem);
    }

    @Test
    public void should_parse_delete() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/document/bulk/bulk_item_delete.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        BulkItem bulkItem = BulkItem.parse(parser);

        assertDelete(bulkItem);
    }

    protected static void assertUpdate(BulkItem bulkItem) {
        assertThat(bulkItem.getOpType()).isEqualTo("update");
        assertThat(bulkItem.getIndex()).isEqualTo("index1");
        assertThat(bulkItem.getType()).isEqualTo("type1");
        assertThat(bulkItem.getId()).isEqualTo("1");
        assertThat(bulkItem.getStatus()).isEqualTo(404);
        assertThat(bulkItem.getError()).isEqualTo("DocumentMissingException[[index1][-1] [type1][1]: document missing]");
        assertThat(bulkItem.getFound()).isNull();
    }

    protected static void assertDelete(BulkItem bulkItem) {
        assertThat(bulkItem.getOpType()).isEqualTo("delete");
        assertThat(bulkItem.getIndex()).isEqualTo("test");
        assertThat(bulkItem.getType()).isEqualTo("type1");
        assertThat(bulkItem.getId()).isEqualTo("2");
        assertThat(bulkItem.getVersion()).isEqualTo(3);
        assertThat(bulkItem.getStatus()).isEqualTo(404);
        assertThat(bulkItem.getError()).isNull();
        assertThat(bulkItem.getFound()).isFalse();
    }

    protected static void assertCreate(BulkItem bulkItem) {
        assertThat(bulkItem.getOpType()).isEqualTo("create");
        assertThat(bulkItem.getIndex()).isEqualTo("test");
        assertThat(bulkItem.getType()).isEqualTo("type1");
        assertThat(bulkItem.getId()).isEqualTo("3");
        assertThat(bulkItem.getStatus()).isEqualTo(409);
        assertThat(bulkItem.getError()).isEqualTo("DocumentAlreadyExistsException[[test][4] [type1][3]: document already exists]");
        assertThat(bulkItem.getFound()).isNull();
    }

    protected static void assertIndex(BulkItem bulkItem) {
        assertThat(bulkItem.getOpType()).isEqualTo("index");
        assertThat(bulkItem.getIndex()).isEqualTo("test");
        assertThat(bulkItem.getType()).isEqualTo("type1");
        assertThat(bulkItem.getId()).isEqualTo("1");
        assertThat(bulkItem.getVersion()).isEqualTo(3);
        assertThat(bulkItem.getStatus()).isEqualTo(200);
        assertThat(bulkItem.getError()).isNull();
        assertThat(bulkItem.getFound()).isNull();
    }
}