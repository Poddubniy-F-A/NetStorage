import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class ServerHandler extends SimpleChannelInboundHandler<Message> {
    private final Path rootDir = new File(
            new File(new File(Objects.requireNonNull(Server.class.getResource("/")).getPath()).getParent()).getParent()
    ).toPath().getFileName();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        switch (msg.command()) {
            case Message.PUT_COMMAND -> putCase(ctx, msg);
            case Message.EXPORT_COMMAND -> exportCase(ctx, msg);
            case Message.GET_COMMAND -> getCase(ctx, msg);
            case Message.IMPORT_COMMAND -> importCase(ctx, msg);
            case Message.DELETE_COMMAND -> deleteCase(ctx, msg);
        }
    }

    private void putCase(ChannelHandlerContext ctx, Message requestMsg) {
        Path path = rootDir.resolve(requestMsg.file().toPath());

        try {
            Files.createDirectories(path.getParent());
            try {
                Files.createFile(path);
                try {
                    Files.write(path, requestMsg.data());

                    sendSuccessCallback(ctx);
                } catch (IOException e) {
                    handleException(ctx, e);
                }
            } catch (IOException e) {
                sendFailureCallback(ctx);
            }
        } catch (IOException e) {
            handleException(ctx, e);
        }
    }

    private void exportCase(ChannelHandlerContext ctx, Message requestMsg) {
        try {
            Files.write(rootDir.resolve(requestMsg.file().toPath()), requestMsg.data(), StandardOpenOption.APPEND);

            sendSuccessCallback(ctx);
        } catch (IOException e) {
            handleException(ctx, e);
        }
    }

    private void getCase(ChannelHandlerContext ctx, Message requestMsg) {
        if (Files.exists(rootDir.resolve(requestMsg.file().toPath()))) {
            sendSuccessCallback(ctx);
        } else {
            sendFailureCallback(ctx);
        }
    }

    private void importCase(ChannelHandlerContext ctx, Message requestMsg) {
        Path path = rootDir.resolve(requestMsg.file().toPath());
        File file = new File(path.toString());

        long clientFileSize = Long.parseLong(new String(requestMsg.data()));
        long delta = file.length() - clientFileSize;
        if (delta >= 0) {
            try {
                FileInputStream fileStream = new FileInputStream(file);
                fileStream.getChannel().position(clientFileSize);

                byte[] data = new byte[(int) Math.min(Server.MAX_DATA_SIZE, delta)];
                if (fileStream.read(data) != -1) {
                    ChannelFuture future = ctx.writeAndFlush(new Message(Message.IMPORT_COMMAND, requestMsg.file(), data));
                    future.addListener(ChannelFutureListener.CLOSE);
                } else {
                    handleException(ctx, new RuntimeException("Ошибка при считывании данных из файла " + file));
                }
            } catch (IOException e) {
                handleException(ctx, e);
            }
        } else {
            sendFailureCallback(ctx);
        }
    }

    private void deleteCase(ChannelHandlerContext ctx, Message requestMsg) {
        try {
            Files.delete(rootDir.resolve(requestMsg.file().toPath()));

            sendSuccessCallback(ctx);
        } catch (IOException e) {
            sendFailureCallback(ctx);
        }
    }

    private void sendSuccessCallback(ChannelHandlerContext ctx) {
        ChannelFuture future = ctx.writeAndFlush(new Message(Message.REPORT_COMMAND, null, new byte[]{Server.SUCCESS_CODE}));
        future.addListener(ChannelFutureListener.CLOSE);
    }

    private void sendFailureCallback(ChannelHandlerContext ctx) {
        ChannelFuture future = ctx.writeAndFlush(new Message(Message.REPORT_COMMAND, null, new byte[]{Server.FAILURE_CODE}));
        future.addListener(ChannelFutureListener.CLOSE);
    }

    private void handleException(ChannelHandlerContext ctx, Exception e) {
        ChannelFuture future = ctx.writeAndFlush(new Message(Message.CRUSH_COMMAND, null, new byte[]{}));
        future.addListener(ChannelFutureListener.CLOSE);

        throw new RuntimeException(e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}