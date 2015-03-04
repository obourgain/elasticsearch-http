package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class TokenTest {

    @Test
    public void should_parse_token() throws Exception {
        String json = readFromClasspath("json/termvector/token.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        Token token = new Token().parse(parser);

        assertToken(token);
    }

    @Test
    public void testParseList() throws Exception {
        String json = readFromClasspath("json/termvector/token2.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        List<Token> tokens = Token.parseList(parser);

        assertThat(tokens).hasSize(3);
        Token token = tokens.get(0);
        assertToken(token);
    }

    public static void assertToken(Token token) {
        assertThat(token.getStartOffset()).isEqualTo(8);
        assertThat(token.getEndOffset()).isEqualTo(12);
        assertThat(token.getPosition()).isEqualTo(1);
        assertThat(token.getPayload()).isEqualTo("d29yZA==");
    }

    // dependant of the json, see term.json
    public static void assertTestToken(List<Token> tokens) {
        assertThat(tokens).hasSize(3);

        Token token1 = tokens.get(0);
        assertThat(token1.getEndOffset()).isEqualTo(12);
        assertThat(token1.getPosition()).isEqualTo(1);
        assertThat(token1.getStartOffset()).isEqualTo(8);
        assertThat(token1.getPayload()).isEqualTo("d29yZA==");

        Token token2 = tokens.get(1);
        assertThat(token2.getEndOffset()).isEqualTo(17);
        assertThat(token2.getPosition()).isEqualTo(2);
        assertThat(token2.getStartOffset()).isEqualTo(13);
        assertThat(token2.getPayload()).isEqualTo("d29yZA==");

        Token token3 = tokens.get(2);
        assertThat(token3.getEndOffset()).isEqualTo(22);
        assertThat(token3.getPosition()).isEqualTo(3);
        assertThat(token3.getStartOffset()).isEqualTo(18);
        assertThat(token3.getPayload()).isEqualTo("d29yZA==");
    }

    public static void assertTwitterToken(List<Token> tokens) {
        assertThat(tokens).hasSize(1);
        Token token = tokens.get(0);
        assertThat(token.getEndOffset()).isEqualTo(7);
        assertThat(token.getPosition()).isEqualTo(0);
        assertThat(token.getStartOffset()).isEqualTo(0);
        assertThat(token.getPayload()).isEqualTo("d29yZA==");
    }
}