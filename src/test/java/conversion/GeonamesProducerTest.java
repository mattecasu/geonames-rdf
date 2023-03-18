package conversion;import convertion.GeonamesProducer;import java.io.IOException;

public class GeonamesProducerTest {

  public void test()throws IOException {
    new GeonamesProducer().populateCodes().collectParents().labels().features();
  }

}
