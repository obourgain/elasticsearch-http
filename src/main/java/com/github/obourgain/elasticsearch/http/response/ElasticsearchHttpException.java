package com.github.obourgain.elasticsearch.http.response;

public class ElasticsearchHttpException extends RuntimeException {

    private final int statusCode;

    public ElasticsearchHttpException(int statusCode) {
        this.statusCode = statusCode;
    }

    public ElasticsearchHttpException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public ElasticsearchHttpException(String message, Throwable cause, int statusCode) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public ElasticsearchHttpException(Throwable cause, int statusCode) {
        super(cause);
        this.statusCode = statusCode;
    }

    public ElasticsearchHttpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, int statusCode) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.statusCode = statusCode;
    }

    @Override
    public String getMessage() {
        return "status code " + statusCode + " " + super.getMessage();
    }

}
