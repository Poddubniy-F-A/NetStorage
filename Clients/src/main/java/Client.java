import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Scanner;

public class Client {
    private static final Scanner scanner = new Scanner(System.in);
    private static final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private static DataInputStream inputStream;
    private static OutputStream outputStream;
    private static DataOutput dataOutputStream;

    private static Path rootDir;

    public static void main(String[] args) throws IOException, InterruptedException {
        final String EXIT_COMMAND = "-1";

        Socket socket = new Socket("localhost", Server.CONNECTION_PORT);
        inputStream = new DataInputStream(socket.getInputStream());
        outputStream = socket.getOutputStream();
        dataOutputStream = new DataOutputStream(outputStream);

        connect();
        authorize();
        while (true) {
            System.out.println("Введите\n" +
                    Message.PUT_COMMAND + " - если хотите отправить копию файла на сервер\n" +
                    Message.GET_COMMAND + " - если хотите получить файл с сервера\n" +
                    Message.DELETE_COMMAND + " - если хотите удалить файл с сервера\n" +
                    "и путь к нужному файлу\n" +
                    "Если хотите завершить работу, введите " + EXIT_COMMAND);

            switch (scanner.next()) {
                case Message.PUT_COMMAND -> putCase();
                case Message.GET_COMMAND -> getCase();
                case Message.DELETE_COMMAND -> deleteCase();
                case EXIT_COMMAND -> {
                    disconnect();

                    workerGroup.shutdownGracefully();
                    return;
                }
                default -> System.out.println("Некорректный ввод\n");
            }
        }
    }

    private static void connect() throws IOException {
        int connectionStatus = inputStream.readInt();
        if (connectionStatus != Server.SUCCESS_CODE) {
            System.out.println("Достигнут лимит пользователей на сервере\nПожалуйста, ожидайте...\n");

            do {
                System.out.println("Ваше место в очереди: " + connectionStatus);
            } while ((connectionStatus = inputStream.readInt()) != Server.SUCCESS_CODE);
        }
        System.out.println("Вы подключены\n");
    }

    private static void authorize() throws IOException {
        final String AUTH_COMMAND = "A";
        final String REG_COMMAND = "R";

        while (true) {
            System.out.println("Введите\n" +
                    AUTH_COMMAND + " - если вы зарегистрированный пользователь\n" +
                    REG_COMMAND + " - если для входа в систему Вам надо зарегистрироваться");
            switch (scanner.next()) {
                case AUTH_COMMAND -> {
                    dataOutputStream.writeInt(Server.SUCCESS_CODE);
                    authenticate();
                    return;
                }
                case REG_COMMAND -> {
                    dataOutputStream.writeInt(Server.FAILURE_CODE);
                    register();
                    return;
                }
                default -> System.out.println("Некорректный ввод\n");
            }
        }
    }

    private static void authenticate() throws IOException {
        while (true) {
            System.out.println("Введите свой логин");
            String login = scanner.next();
            writeStringIntoStream(login);

            switch (inputStream.readInt()) {
                case Server.FAILURE_CODE -> System.out.println("Пользователь с таким логином не зарегистрирован\n");
                case Server.SUCCESS_CODE -> {
                    while (true) {
                        System.out.println("Введите пароль");
                        writeStringIntoStream(scanner.next());

                        switch (inputStream.readInt()) {
                            case Server.FAILURE_CODE -> System.out.println("Неверный пароль\n");
                            case Server.SUCCESS_CODE -> {
                                initRootDir(login);

                                System.out.println("Аутентификация пройдена\n");
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    private static void register() throws IOException {
        while (true) {
            System.out.println("Установите логин");
            String login = scanner.next();
            writeStringIntoStream(login);

            switch (inputStream.readInt()) {
                case Server.FAILURE_CODE -> System.out.println("Пользователь с таким логином уже зарегистрирован\n");
                case Server.SUCCESS_CODE -> {
                    System.out.println("Установите пароль");
                    writeStringIntoStream(scanner.next());

                    initRootDir(login);
                    Files.createDirectories(rootDir);

                    System.out.println("Вы зарегистрированы\n");
                    return;
                }
            }
        }
    }

    private static void initRootDir(String login) {
        rootDir = Path.of(
                new File(
                        new File(new File(Objects.requireNonNull(Client.class.getResource("/")).getPath()).getParent()).getParent()
                ).toPath().getFileName() + "/" + login
        );
    }

    private static void writeStringIntoStream(String string) throws IOException {
        dataOutputStream.writeInt(string.length());
        outputStream.write(string.getBytes(StandardCharsets.UTF_8));
    }

    private static void putCase() throws InterruptedException, IOException {
        File file = new File(rootDir + "/" + scanner.next());
        if (!file.exists()) {
            System.out.println("Некорректный путь к файлу\n");
        } else if (!requestIsGranted(new Message(Message.PUT_COMMAND, file, new byte[]{}))) {
            System.out.println("Файл с таким именем уже существует на сервере\n");
        } else {
            exportFile(file);
        }
    }

    private static void exportFile(File file) throws IOException, InterruptedException {
        FileInputStream inputStream = new FileInputStream(file);

        long size = file.length();
        for (long pos = 0; pos < size; pos += Server.MAX_DATA_SIZE) {
            inputStream.getChannel().position(pos);
            byte[] data = new byte[(int) Math.min(Server.MAX_DATA_SIZE, size - pos)];
            if (inputStream.read(data) == -1) {
                throw new RuntimeException("Ошибка при считывании данных из файла " + file);
            } else {
                requestIsGranted(new Message(Message.EXPORT_COMMAND, file, data));
            }
        }
        System.out.println("Файл успешно создан\n");
    }

    private static void getCase() throws InterruptedException, IOException {
        File file = new File(rootDir + "/" + scanner.next());
        if (file.exists()) {
            System.out.println("Файл с таким именем уже есть на устройстве\n");
        } else if (!requestIsGranted(new Message(Message.GET_COMMAND, file, null))) {
            System.out.println("Файла по указанному пути не существует\n");
        } else {
            importFile(file);
        }
    }

    private static void importFile(File file) throws IOException, InterruptedException {
        Path path = file.toPath();
        try {
            if (!file.createNewFile()) {
                throw new RuntimeException("Ошибка при создании файла " + path);
            }
        } catch (IOException e) {
            Files.createDirectories(path.getParent());
            if (!file.createNewFile()) {
                throw new RuntimeException("Ошибка при создании файла " + path);
            }
        }

        long curFileSize = 0;
        while (requestIsGranted(new Message(Message.IMPORT_COMMAND, file, String.valueOf(curFileSize).getBytes(StandardCharsets.UTF_8)))) {
            curFileSize += Server.MAX_DATA_SIZE;
        }
        System.out.println("Файл успешно скопирован\n");
    }

    private static void deleteCase() throws InterruptedException {
        if (!requestIsGranted(new Message(Message.DELETE_COMMAND, new File(rootDir + "/" + scanner.next()), null))) {
            System.out.println("Файла по указанному пути не существует\n");
        } else {
            System.out.println("Файл успешно удалён\n");
        }
    }

    private static boolean requestIsGranted(Message request) throws InterruptedException {
        int[] executionCode = new int[1];

        Bootstrap sender = new Bootstrap();
        sender.group(workerGroup);
        sender.channel(NioSocketChannel.class);
        sender.option(ChannelOption.SO_KEEPALIVE, true);
        sender.handler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(Server.MAX_OBJECT_SIZE, ClassResolvers.cacheDisabled(null)),
                        new ClientHandler(request, ((code) -> executionCode[0] = code))
                );
            }
        });

        ChannelFuture future = sender.connect("localhost", Server.SERVER_PORT).sync();
        future.channel().closeFuture().sync();

        return executionCode[0] == Server.SUCCESS_CODE;
    }

    private static void disconnect() throws IOException {
        dataOutputStream.writeInt(Server.SUCCESS_CODE);
        System.out.println("Завершение работы...");
    }
}