package com.github.obourgain.elasticsearch.http.concurrent;

import java.util.Objects;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.common.hppc.IntOpenHashSet;
import org.elasticsearch.common.hppc.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;

/**
 * A {@link AsyncCompletionHandler} that notifies a {@link ActionListener} on complete or failure.
 *
 * @author olivier bourgain
 */
public abstract class ListenerAsyncCompletionHandler<Resp> extends AsyncCompletionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ListenerAsyncCompletionHandler.class);

    private final ActionListener<Resp> listener;

    public ListenerAsyncCompletionHandler(ActionListener<Resp> listener) {
        this.listener = listener;
    }

    public ListenerAsyncCompletionHandler(Object request, ActionListener<Resp> listener) {
        throw new RuntimeException("do not call");
    }

    @Override
    public Resp onCompleted(Response response) {
        ErrorHandler.checkError(response, non200ValidStatuses());
        Resp resp = convert(response);
        // when overriding, do not forget to call listener.onResponse() !
        listener.onResponse(resp);
        return resp;
    }

    @Override
    public void onThrowable(Throwable t) {
        listener.onFailure(t);
    }

    protected Resp convert(Response response) {
        throw new RuntimeException("to implement");
    }

    protected Resp convert(ResponseWrapper responseWrapper) {
        throw new RuntimeException("to implement");
    }

    private static final IntSet empty = new IntOpenHashSet();

    protected IntSet non200ValidStatuses() {
        return empty;
    }
}
