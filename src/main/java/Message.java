import java.io.File;
import java.io.Serializable;

public record Message(String command, File file, byte[] data) implements Serializable {
    public static final String PUT_COMMAND = "put";
    public static final String GET_COMMAND = "get";
    public static final String DELETE_COMMAND = "del";

    public static final String EXPORT_COMMAND = "exp";
    public static final String IMPORT_COMMAND = "imp";

    public static final String REPORT_COMMAND = "rep";
    public static final String CRUSH_COMMAND = "cr";
}