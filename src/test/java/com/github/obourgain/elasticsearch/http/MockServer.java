package com.github.obourgain.elasticsearch.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import org.junit.Test;

public class MockServer {

    @Test
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(9900);
        Socket socket = serverSocket.accept();

        System.out.println("accepted");

        InputStream inputStream = socket.getInputStream();
        int read;
        byte[] buffer = new byte[4096];
        while((read = inputStream.read(buffer)) != -1) {
//            System.out.println(read);
//            System.out.println(Arrays.toString(buffer));
            System.out.println(new String(buffer, 0, read));
        }
    }

}
