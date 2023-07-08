import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Server {
    public static final int MAX_OBJECT_SIZE = 1024 * 1024;
    public static final int MAX_DATA_SIZE = MAX_OBJECT_SIZE - 2 * 1024;

    public static final int SERVER_PORT = 10000;
    public static final int CONNECTION_PORT = 10001;

    public static final int SUCCESS_CODE = 0;
    public static final int FAILURE_CODE = 1;

    private static Connection sqlConnection;

    private static volatile boolean wasNewConnection;
    private static final ArrayList<Integer> queue = new ArrayList<>();
    private static int connections = 0;

    public static void main(String[] args) throws SQLException, IOException {
        new Thread(() -> {
            try {
                processServer();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        sqlConnection = DriverManager.getConnection("jdbc:sqlite:DataBase.db");

        ServerSocket connector = new ServerSocket(CONNECTION_PORT);
        while (true) {
            wasNewConnection = false;

            new Thread(() -> {
                try {
                    Socket socket = connector.accept();
                    wasNewConnection = true;

                    DataInput inputStream = new DataInputStream(socket.getInputStream());
                    DataOutput outputStream = new DataOutputStream(socket.getOutputStream());

                    connect(outputStream);
                    authorize(inputStream, outputStream);
                    disconnect(inputStream);

                    socket.close();
                } catch (IOException | SQLException e) {
                    throw new RuntimeException(e);
                }
            }).start();

            while (!wasNewConnection) {
                Thread.onSpinWait();
            }
        }
    }

    private static void processServer() throws InterruptedException {
        final int N_THREADS = 1;
        final int OPTION_VALUE = 128;

        EventLoopGroup bossGroup = new NioEventLoopGroup(N_THREADS);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap server = new ServerBootstrap();
        server.group(bossGroup, workerGroup);
        server.channel(NioServerSocketChannel.class);
        server.option(ChannelOption.SO_BACKLOG, OPTION_VALUE);
        server.childOption(ChannelOption.SO_KEEPALIVE, true);
        server.childHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(MAX_OBJECT_SIZE, ClassResolvers.cacheDisabled(null)),
                        new ServerHandler()
                );
            }
        });

        ChannelFuture future = server.bind(SERVER_PORT).sync();
        future.channel().closeFuture().sync();

        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private static void connect(DataOutput outputStream) throws IOException {
        final int MAX_USERS_NUM = 2;

        Integer hash = outputStream.hashCode();
        queue.add(hash);

        for (int index = queue.indexOf(hash); index > 0; index = queue.indexOf(hash)) {
            outputStream.writeInt(queue.indexOf(hash) + 1);
            while (queue.indexOf(hash) == index) {
                Thread.onSpinWait();
            }
        }
        if (connections == MAX_USERS_NUM) {
            outputStream.writeInt(1);
            while (connections == MAX_USERS_NUM) {
                Thread.onSpinWait();
            }
        }
        outputStream.writeInt(SUCCESS_CODE);

        connections++;
        queue.remove(hash);
    }

    private static void authorize(DataInput inputStream, DataOutput outputStream) throws IOException, SQLException {
        switch (inputStream.readInt()) {
            case SUCCESS_CODE -> authenticate(inputStream, outputStream);
            case FAILURE_CODE -> register(inputStream, outputStream);
        }
    }

    private static void authenticate(DataInput inputStream, DataOutput outputStream) throws SQLException, IOException {
        Statement sqlStatement = sqlConnection.createStatement();

        String correctPassword;
        while ((correctPassword = sqlStatement.executeQuery(
                String.format("SELECT Password FROM LogPass WHERE Login = '%s';", stringFromStream(inputStream))
        ).getString(1)) == null) {
            outputStream.writeInt(FAILURE_CODE);
        }
        outputStream.writeInt(SUCCESS_CODE);

        while (!stringFromStream(inputStream).equals(correctPassword)) {
            outputStream.writeInt(FAILURE_CODE);
        }
        outputStream.writeInt(SUCCESS_CODE);
    }

    private static void register(DataInput inputStream, DataOutput outputStream) throws SQLException, IOException {
        Statement sqlStatement = sqlConnection.createStatement();

        String login;
        while (sqlStatement.executeQuery(
                String.format("SELECT Password FROM LogPass WHERE Login = '%s';", login = stringFromStream(inputStream))
        ).getString(1) != null) {
            outputStream.writeInt(FAILURE_CODE);
        }
        outputStream.writeInt(SUCCESS_CODE);

        sqlStatement.execute(String.format("INSERT INTO LogPass (Login, Password) VALUES ('%s', '%s');", login, stringFromStream(inputStream)));
    }

    private static String stringFromStream(DataInput inputStream) throws IOException {
        byte[] stringBytes = new byte[inputStream.readInt()];
        inputStream.readFully(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    private static void disconnect(DataInput inputStream) throws IOException {
        inputStream.readInt();
        connections--;
    }
}