package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Matches implements Iterable<Match> {

    private List<Match> matches = new ArrayList<>();

    @Override
    public Iterator<Match> iterator() {
        return matches.iterator();
    }

    public Matches parse(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    Match match = new Match().parse(parser);
                    matches.add(match);
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
