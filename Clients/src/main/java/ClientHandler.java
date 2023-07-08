import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Consumer;

public class ClientHandler extends SimpleChannelInboundHandler<Message> {
    private final Message requestMsg;
    private final Consumer<Integer> callback;

    public ClientHandler(Message msg, Consumer<Integer> callback) {
        this.requestMsg = msg;
        this.callback = callback;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(requestMsg);

        if (!Objects.equals(requestMsg.command(), Message.EXPORT_COMMAND) && !Objects.equals(requestMsg.command(), Message.IMPORT_COMMAND)) {
            System.out.println("Идёт обмен данными с сервером...");
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws IOException {
        switch (msg.command()) {
            case Message.REPORT_COMMAND -> reportCase(msg);
            case Message.IMPORT_COMMAND -> importCase(msg);
            case Message.CRUSH_COMMAND -> crushCase();
        }
    }

    private void reportCase(Message reportMsg) {
        callback.accept((int) reportMsg.data()[0]);
    }

    private void importCase(Message importMsg) throws IOException {
        Files.write(importMsg.file().toPath(), importMsg.data(), StandardOpenOption.APPEND);

        callback.accept(Server.SUCCESS_CODE);
    }

    private void crushCase() {
        throw new RuntimeException("Ошибка выполнения на сервере");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}