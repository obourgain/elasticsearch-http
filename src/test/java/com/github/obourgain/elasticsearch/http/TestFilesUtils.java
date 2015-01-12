package com.github.obourgain.elasticsearch.http;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.google.common.io.Resources;

public class TestFilesUtils {

    public static String readFromClasspath(String name) {
        URL resource = Resources.getResource(name);
        try {
            return new String(Files.readAllBytes(Paths.get(resource.getFile())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
