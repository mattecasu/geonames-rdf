package conversion;

import static conversion.GeonamesHeaders.admin1code;
import static conversion.GeonamesHeaders.admin2code;
import static conversion.GeonamesHeaders.admin3code;
import static conversion.GeonamesHeaders.admin4code;
import static conversion.GeonamesHeaders.altitude;
import static conversion.GeonamesHeaders.countryCode;
import static conversion.GeonamesHeaders.elevation;
import static conversion.GeonamesHeaders.featureClass;
import static conversion.GeonamesHeaders.featureCode;
import static conversion.GeonamesHeaders.geonameid;
import static conversion.GeonamesHeaders.latitude;
import static conversion.GeonamesHeaders.longitude;
import static conversion.GeonamesHeaders.modificationDate;
import static conversion.GeonamesHeaders.name;
import static conversion.GeonamesHeaders.population;
import static conversion.GeonamesHeaders.timezone;
import static namespaces.Namespaces.NS_GEONAMES_INSTANCES;
import static org.eclipse.rdf4j.model.util.Values.iri;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.eclipse.rdf4j.model.IRI;

@Accessors(chain = true)
@Getter
public class GeonamesFeature {

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
