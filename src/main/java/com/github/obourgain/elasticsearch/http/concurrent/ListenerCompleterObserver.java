package com.github.obourgain.elasticsearch.http.concurrent;

import org.elasticsearch.action.ActionListener;
import rx.Observer;

public class ListenerCompleterObserver<T> implements Observer<T> {

    private final ActionListener<T> listener;

    public ListenerCompleterObserver(ActionListener<T> listener) {
        this.listener = listener;
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onError(Throwable e) {
        listener.onFailure(e);
    }

    @Override
    public void onNext(T response) {
        listener.onResponse(response);
    }
}