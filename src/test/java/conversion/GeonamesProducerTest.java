package conversion;

import org.junit.Ignore;
import org.junit.Test;

public class GeonamesProducerTest {

  @Test
  @Ignore
  public void test() throws Exception {
    new GeonamesProducer("src/test/resources/input_source", "src/test/resources/output")
        .populateCodes()
        .collectParents()
        .collectLabels()
        .features();
  }
}
