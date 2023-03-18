package conversion;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;
import namespaces.Namespaces;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.collect.Maps.newHashMap;
import static java.nio.file.Files.newOutputStream;
import static lombok.AccessLevel.PRIVATE;
import static namespaces.Namespaces.GN_ONTO;
import static namespaces.Namespaces.NS_CUSTOM;
import static namespaces.Namespaces.NS_DCTERMS;
import static namespaces.Namespaces.NS_FOAF;
import static namespaces.Namespaces.NS_GEONAMES_INSTANCES;
import static namespaces.Namespaces.NS_WGS_SCHEMA;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/*
 * inspired by https://github.com/europeana/tools/tree/master/trunk/annocultor/converters/geonames
 */

@Accessors(chain = true)
public class GeonamesProducer {

  private final Set<Namespace> namespaces = Namespaces.getNamespaces();

  private String input_source;
  private String output;

  private Multimap<String, String> links = ArrayListMultimap.create();
  private Multimap<String, String> broaders = ArrayListMultimap.create();
  private Multimap<String, String> broadersAdm = ArrayListMultimap.create();

  Map<String, TurtleWriter> files = newHashMap();
  TurtleWriter allCountries = null;

  ConcurrentMap<String, String> adminsToIdsMap;
  static final Logger logger = LoggerFactory.getLogger(GeonamesProducer.class);

  SimpleValueFactory factory = SimpleValueFactory.getInstance();

  public GeonamesProducer(String input_source, String output) {
    this.input_source = input_source;
    this.output = output;
    IoUtils.createDir(output);
    DB db = DBMaker.fileDB(output + "/mapFile").make();
    adminsToIdsMap = db.hashMap("map", Serializer.STRING, Serializer.STRING).create();
  }

  private void write(String country, Statement triple, boolean isDescriptionOfCountry) {
    // String continent = countryToContinent.getProperty(country);
    if (isDescriptionOfCountry) {
      if (allCountries == null) {
        allCountries = IoUtils.getWriter(output + "/all-countries.ttl");
        allCountries.startRDF();
        namespaces.forEach(ns -> allCountries.handleNamespace(ns.getPrefix(), ns.getName()));
      }
      allCountries.handleStatement(triple);
    }

    TurtleWriter writer = files.get(country);
    if (writer == null) {
      country = country.equals("") ? "noCountry" : country;
      writer = IoUtils.getWriter(output + "/" + country + ".ttl");
      files.put(country, writer);
      writer.startRDF();
      for (Namespace ns : namespaces) {
        writer.handleNamespace(ns.getPrefix(), ns.getName());
      }
    }
    writer.handleStatement(triple);
  }

  protected GeonamesProducer labels() throws Exception {
    TurtleWriter writer = new TurtleWriter(newOutputStream(Paths.get(output, "altLabels.ttl")));

    writer.startRDF();

    logger.info("Loading alt names ");
    long count = 0;
    LineIterator it = FileUtils.lineIterator(new File(input_source, "alternateNames.txt"), "UTF-8");

    String text;

    while (it.hasNext()) {
      text = it.nextLine();

      String[] fields = text.split("\t");
      String code = fields[1];
      String lang = fields[2];
      String label = fields[3];

      boolean isPreferred = fields.length > 4 && fields[4].equals("1");
      boolean isShort = fields.length > 5 && fields[5].equals("1");
      boolean isColloquial = fields.length > 6 && fields[6].equals("1");
      boolean isHistoric = fields.length > 7 && fields[7].equals("1");

      if ("link".equals(lang)) {
        // wikipedia links
        links.put(code, label);
      } else {
        if (lang.length() < 3) {

          String property = GN_ONTO + "alternateName";
          property = isPreferred ? GN_ONTO + "officialName" : property;
          property = isShort ? GN_ONTO + "shortName" : property;
          property = isColloquial ? GN_ONTO + "colloquialName" : property;
          property = isHistoric ? GN_ONTO + "historicalName" : property;

          Literal literal =
              lang.equals("") ? factory.createLiteral(label) : factory.createLiteral(label, lang);

          Statement st =
              factory.createStatement(
                  factory.createIRI(codeToUri(code)), factory.createIRI(property), literal);
          writer.handleStatement(st);

        } else {
          // postcodes
        }
      }
      count++;

      if (count % 100000 == 0) {
        logger.info(String.format("Processed %s altNames", count));
      }
    }
    it.close();
    writer.endRDF();
    return this;
  }

  private String codeToUri(String code) {
    return NS_GEONAMES_INSTANCES + code + "/";
  }

  protected GeonamesProducer collectParents() throws Exception {

    logger.info("Loading parents");

    Stream<String> it =
        Files.lines(Paths.get(input_source, "hierarchy.txt"), Charset.forName("UTF-8"));

    it.forEach(
        line -> {
          String[] fields = line.split("\t");
          String parent = fields[0];
          String child = fields[1];
          boolean adm = false;
          if (fields.length > 2) {
            adm = fields[2].equals("ADM");
          }
          if (adm) {
            broadersAdm.put(child, parent);
          } else {
            broaders.put(child, parent);
          }
        });

    it.close();
    return this;
  }

  protected GeonamesProducer features() throws Exception {
    logger.info("Parsing features");

    //     createDirsForContinents();

    final AtomicInteger counter = new AtomicInteger(0);
    Stream<String> it =
        Files.lines(Paths.get(input_source, "allCountries.txt"), StandardCharsets.UTF_8);

    it.forEach(
        line -> {
          GeonamesFeature feature = new GeonamesFeature(line);

          // progress
          counter.incrementAndGet();
          if (counter.get() % 100000 == 0) {
            logger.info(String.format("Processed %s features", counter));
          }
          boolean isDescriptionOfCountry = feature.getFeatureCodeField().startsWith("A.PCLI");

          if (isNotEmpty(feature.getNameValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(GN_ONTO + "name"),
                    factory.createLiteral(feature.getNameValue()));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }

          if (feature.getPopulationValue().length() > 1) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(GN_ONTO + "population"),
                    factory.createLiteral(feature.getPopulationValue(), XSD.INTEGER));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getLongValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_WGS_SCHEMA + "long"),
                    factory.createLiteral(feature.getLongValue(), XSD.DECIMAL));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getLatValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_WGS_SCHEMA + "lat"),
                    factory.createLiteral(feature.getLatValue(), XSD.DECIMAL));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getAltValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_WGS_SCHEMA + "alt"),
                    factory.createLiteral(feature.getAltValue(), XSD.DECIMAL));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getElevationValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_CUSTOM + "gtopo30"),
                    factory.createLiteral(feature.getElevationValue(), XSD.DECIMAL));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getFeatureClassField())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(GN_ONTO + "featureClass"),
                    factory.createIRI(GN_ONTO + feature.getFeatureClassField()));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getFeatureCodeField())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(GN_ONTO + "featureCode"),
                    factory.createIRI(GN_ONTO + feature.getFeatureCodeField()));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getCountry())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(GN_ONTO + "countryCode"),
                    factory.createLiteral(feature.getCountry()));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }

          if (isNotEmpty(feature.getTimezoneValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_CUSTOM + "timezone"),
                    factory.createLiteral(feature.getTimezoneValue()));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }
          if (isNotEmpty(feature.getModificationDateValue())) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_DCTERMS + "modified"),
                    factory.createLiteral(feature.getModificationDateValue(), XSD.DATE));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }

          if (isNotEmpty(feature.getAdmin2Value())
              && feature.getFeatureCodeField().equals("A.ADM2")) {
            Statement triple =
                factory.createStatement(
                    feature.getSubject(),
                    factory.createIRI(NS_CUSTOM + "admin2"),
                    factory.createLiteral(feature.getAdmin2Value()));
            write(feature.getCountry(), triple, isDescriptionOfCountry);
          }

          if (links.containsKey(feature.getId())) {
            for (String link : links.get(feature.getId())) {
              String property =
                  link.contains("wikipedia") ? GN_ONTO + "wikipediaArticle" : NS_FOAF + "page";
              Statement triple =
                  factory.createStatement(
                      feature.getSubject(),
                      factory.createIRI(property),
                      factory.createLiteral(link));
              write(feature.getCountry(), triple, isDescriptionOfCountry);
            }
            links.removeAll(feature.getId());
          }

          if (broaders.containsKey(feature.getId())) {
            for (Object broaderCode : broaders.get(feature.getId())) {
              String broader = (String) broaderCode;
              Statement triple =
                  factory.createStatement(
                      feature.getSubject(),
                      factory.createIRI(GN_ONTO + "locatedIn"),
                      factory.createIRI(codeToUri(broader)));
              write(feature.getCountry(), triple, isDescriptionOfCountry);
            }
            broaders.removeAll(feature.getId());
          }

          if (broadersAdm.containsKey(feature.getId())) {
            for (Object broaderCode : broadersAdm.get(feature.getId())) {
              String broader = (String) broaderCode;
              Statement triple =
                  factory.createStatement(
                      feature.getSubject(),
                      factory.createIRI(GN_ONTO + "parentFeature"),
                      factory.createIRI(codeToUri(broader)));
              write(feature.getCountry(), triple, isDescriptionOfCountry);
            }
            broadersAdm.removeAll(feature.getId());
          }

          // fields admin1... admin4
          String denormalizedAdmins =
              feature.getCountry()
                  + feature.getAdmin1Value()
                  + feature.getAdmin2Value()
                  + feature.getAdmin3Value()
                  + feature.getAdmin4Value();

          if (adminsToIdsMap.containsKey(denormalizedAdmins)) {
            String father = codeToUri(adminsToIdsMap.get(denormalizedAdmins));

            if (!father.equals(feature.getUri())) {
              Statement triple =
                  factory.createStatement(
                      factory.createIRI(feature.getUri()),
                      factory.createIRI(GN_ONTO + "parentFeature"),
                      factory.createIRI(father));
              write(feature.getCountry(), triple, isDescriptionOfCountry);
            }
          }
        });

    it.close();

    logger.info("Finished conversion, flushing and closing output files");

    files.keySet().stream()
        .filter(country -> files.containsKey(country))
        .forEach(country -> files.get(country.toString()).endRDF());

    if (allCountries != null) {
      allCountries.endRDF();
    }
    return this;
  }

  protected GeonamesProducer populateCodes() throws IOException {

    LineIterator it = FileUtils.lineIterator(new File(input_source, "allCountries.txt"), "UTF-8");

    while (it.hasNext()) {
      String text = it.nextLine();
      GeonamesFeature feature = new GeonamesFeature(text);

      Long adminsNumber =
          Stream.of(
                  feature.getAdmin1Value(),
                  feature.getAdmin2Value(),
                  feature.getAdmin3Value(),
                  feature.getAdmin4Value())
              .filter(admin -> isNotEmpty(admin))
              .count();

      if (adminsNumber.intValue() == 1 && feature.getFeatureCodeField().equals("ADM1")) {
        adminsToIdsMap.put(feature.getCountry() + feature.getAdmin1Value(), feature.getId());
      }
      if (adminsNumber.intValue() == 2 && feature.getFeatureCodeField().equals("ADM2")) {
        adminsToIdsMap.put(
            feature.getCountry() + feature.getAdmin1Value() + feature.getAdmin2Value(),
            feature.getId());
      }
      if (adminsNumber.intValue() == 3 && feature.getFeatureCodeField().equals("ADM3")) {
        adminsToIdsMap.put(
            feature.getCountry()
                + feature.getAdmin1Value()
                + feature.getAdmin2Value()
                + feature.getAdmin3Value(),
            feature.getId());
      }
      if (adminsNumber.intValue() == 4 && feature.getFeatureCodeField().equals("ADM4")) {
        adminsToIdsMap.put(
            feature.getCountry()
                + feature.getAdmin1Value()
                + feature.getAdmin2Value()
                + feature.getAdmin3Value()
                + feature.getAdmin4Value(),
            feature.getId());
      }
    }
    it.close();
    return this;
  }

  public static void main(String... args) throws Exception {
    new GeonamesProducer("input_source", "output")
        .populateCodes()
        .collectParents()
        .labels()
        .features();
  }
}
