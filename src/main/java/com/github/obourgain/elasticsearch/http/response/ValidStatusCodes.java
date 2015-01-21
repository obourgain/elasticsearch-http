package com.github.obourgain.elasticsearch.http.response;

import org.elasticsearch.common.hppc.IntOpenHashSet;

public class ValidStatusCodes {

    public static IntOpenHashSet _404 = new IntOpenHashSet();
    static {
        _404.add(404);
    }

}
