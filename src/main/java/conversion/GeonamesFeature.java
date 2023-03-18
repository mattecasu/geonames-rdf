package conversion;

import static namespaces.Namespaces.NS_GEONAMES_INSTANCES;
import static org.eclipse.rdf4j.model.util.Values.iri;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.eclipse.rdf4j.model.IRI;

@Accessors(chain = true)
@Getter
public class GeonamesFeature {

  private final int geonameid = 0; // id of record in geonames database
  private final int name = 1; // name
  // private static final int asciiname = 2; // name in plain ascii
  // public static int alternatenames = 3; // alternatenames, comma separated
  private final int latitude = 4; // latitude (wgs84)
  private final int longitude = 5; // longitude (wgs84)
  private final int featureClass = 6;
  private final int featureCode = 7;
  private final int countryCode = 8; // ISO-3166 2-letter country code, 2 characters
  // private static final int cc2 = 9; // alternate country codes, comma separated, ISO-3166
  private final int admin1code = 10; // fipscode, see file admin1Codes.txt for names
  private final int admin2code = 11;
  private final int admin3code = 12;
  private final int admin4code = 13;
  private final int population = 14;
  private final int elevation = 15;
  private final int altitude = 16;
  private final int timezone = 17;
  private final int modificationDate = 18; // yyyy-MM-dd

  private String country;
  private String id;
  private String nameValue;
  private String uri;
  private final IRI subject;
  private final String featureCodeField;
  private final String featureClassField;
  private final String populationValue;

  private final String timezoneValue;
  private final String modificationDateValue;
  private final String latValue;
  private final String longValue;
  private final String altValue;
  private final String elevationValue;
  private final String admin1Value;
  private final String admin2Value;
  private final String admin3Value;
  private final String admin4Value;

  public GeonamesFeature(String line) {
    String[] fields = line.split("\t");
    if (fields.length != 19) {
      //      it.close();
      throw new RuntimeException("::: Field names mismatch on " + line);
    }
    country = fields[countryCode];
    id = fields[geonameid];
    nameValue = fields[name];
    uri = NS_GEONAMES_INSTANCES + id + "/";
    subject = iri(uri);
    featureCodeField = fields[featureClass] + "." + fields[featureCode];
    featureClassField = fields[featureClass];
    populationValue = fields[population];
    timezoneValue = fields[timezone];
    modificationDateValue = fields[modificationDate];
    latValue = fields[latitude];
    longValue = fields[longitude];
    altValue = fields[altitude];
    elevationValue = fields[elevation];
    admin1Value = fields[admin1code];
    admin2Value = fields[admin2code];
    admin3Value = fields[admin3code];
    admin4Value = fields[admin4code];
  }
}
