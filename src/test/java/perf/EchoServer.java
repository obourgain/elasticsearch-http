package perf;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import rx.Observable;

public class EchoServer {

    static ThreadPoolExecutor executor = new ThreadPoolExecutor(12_000, 12_000, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

    public static void main(String[] args) throws IOException, InterruptedException {
//        listenWithPool();

        HttpServer<ByteBuf, ByteBuf> server = RxNetty.createHttpServer(9200, new RequestHandler<ByteBuf, ByteBuf>() {
            @Override
            public Observable<Void> handle(HttpServerRequest<ByteBuf> request, HttpServerResponse<ByteBuf> response) {
                try {
                    response.setStatus(HttpResponseStatus.OK);
                    response.writeString("foo");
                    return response.close();
                } catch (Throwable e) {
                    System.err.println("Server => Error [" + request.getPath() + "] => " + e);
                    response.setStatus(HttpResponseStatus.BAD_REQUEST);
                    response.writeString("Error 500: Bad Request\n");
                    return response.close();
                }
            }
        });

        server.start();
        try {
            MILLISECONDS.sleep(MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void listenWithPool() throws IOException {
        ServerSocket serverSocket = new ServerSocket(9200);

        System.out.println("listening");
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
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

}
