package perf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EchoServer {

    static ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(9200);
        while (true) {
            final Socket socket = serverSocket.accept();

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        InputStream inputStream = socket.getInputStream();
                        OutputStream outputStream = socket.getOutputStream();

                        byte[] buffer = new byte[1024];
                        int read;
                        while ((read = inputStream.read(buffer)) != -1) {
                            outputStream.write(buffer, 0, read);
                        }

                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

}
