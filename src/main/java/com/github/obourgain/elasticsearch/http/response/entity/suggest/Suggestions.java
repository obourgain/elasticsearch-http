package com.github.obourgain.elasticsearch.http.response.entity.suggest;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import com.github.obourgain.elasticsearch.http.response.entity.Converter;

public class Suggestions {

    private final Map<String, Suggestion> parsed = new HashMap<>();
    private final Map<String, XContentBuilder> rawSuggestions = new HashMap<>();

    public Completion getCompletion(final String name) {
        return findOrCreate(name, new Converter<Completion>() {
            @Override
            public Completion convert(XContentParser parser) {
                return Completion.parse(parser, name);
            }
        });
    }

    public Term getTerm(final String name) {
        return findOrCreate(name, new Converter<Term>() {
            @Override
            public Term convert(XContentParser parser) {
                return Term.parse(parser, name);
            }
        });
    }


    public Phrase getPhrase(final String name) {
        return findOrCreate(name, new Converter<Phrase>() {
            @Override
            public Phrase convert(XContentParser parser) {
                return Phrase.parse(parser, name);
            }
        });
    }

    protected void addRawSuggestion(String name, XContentBuilder rawSuggestion) {
        rawSuggestions.put(name, rawSuggestion);
    }

    private <T extends Suggestion> T findOrCreate(String name, Converter<T> converter) {
        @SuppressWarnings("unchecked")
        T t = (T) parsed.get(name);
        if (t != null) {
            return t;
        } else {
            XContentBuilder builder = rawSuggestions.get(name);
            try {
                if (builder != null) {
                    try (XContentParser parser = XContentHelper.createParser(builder.bytes())) {
                        t = converter.convert(parser);
                        parsed.put(name, t);
                        return t;
                    }
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /*
    parses something like : (with the heading parenthesis)

    {
      "song-suggest": {
         "text": "n",
         "completion": {
            "field": "suggest"
         }
      }
   }

     */
    public static Suggestions parse(XContentParser parser) {
        try {
            assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            Suggestions suggestions = new Suggestions();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == START_OBJECT) {
                    try (XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
                        suggestions.addRawSuggestion(currentFieldName, docBuilder.copyCurrentStructure(parser));
                    }
                }
            }
            return suggestions;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    parses something like (e.g. for sub aggs)

    "song-suggest": {
         "text": "n",
         "completion": {
            "field": "suggest"
         }
      }

     */
    protected static Pair<String, XContentBuilder> parseInnerAgg(XContentParser parser, String aggregationName) {
        try {
            assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            docBuilder.copyCurrentStructure(parser);
            docBuilder.close();
            return Pair.of(aggregationName, docBuilder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
