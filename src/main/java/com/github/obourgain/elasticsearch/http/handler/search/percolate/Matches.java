package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class Matches implements Iterable<Match> {

    private List<Match> matches;

    @Override
    public Iterator<Match> iterator() {
        return matches.iterator();
    }

    public static Matches parse(XContentParser parser) {
        try {
            List<Match> parsedMatches = new ArrayList<>();
            assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    Match match = Match.parse(parser);
                    parsedMatches.add(match);
                }
            }
            return builder().matches(parsedMatches).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
