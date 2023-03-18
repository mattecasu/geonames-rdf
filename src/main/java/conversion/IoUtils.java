package conversion;

import static java.nio.file.Files.newOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;

public class IoUtils {

  public static TurtleWriter getWriter(String path) {
    TurtleWriter writer = null;
    try {
      writer = new TurtleWriter(newOutputStream(Paths.get(path)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return writer;
  }

  public static void createDir(String dir) {
    try {
      Files.createDirectory(Paths.get(dir));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
