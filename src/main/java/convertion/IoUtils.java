package convertion;

import static java.nio.file.Files.newOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;

public class IoUtils {

  public static TurtleWriter getWriter(String path) {
    try (OutputStream os = newOutputStream(Paths.get(path))) {
      return new TurtleWriter(os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createDir(String dir) {
    try {
      Files.createDirectory(Paths.get(dir));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
