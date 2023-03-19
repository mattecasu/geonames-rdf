package conversion;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static namespaces.Namespaces.GN_ONTO;
import static namespaces.Namespaces.NS_CUSTOM;
import static namespaces.Namespaces.NS_DCTERMS;
import static namespaces.Namespaces.NS_FOAF;
import static namespaces.Namespaces.NS_GEONAMES_INSTANCES;
import static namespaces.Namespaces.NS_WGS_SCHEMA;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.experimental.Accessors;
import namespaces.Namespaces;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * inspired by https://github.com/europeana/tools/tree/master/trunk/annocultor/converters/geonames
 */

@Accessors(chain = true)
public class GeonamesProducer {

  private final Set<Namespace> namespaces = Namespaces.getNamespaces();

  private final String input_source;
  private final String output;

  private final Multimap<String, String> links =
      Multimaps.synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());
  private final Multimap<String, String> broaders =
      Multimaps.synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());
  private final Multimap<String, String> broadersAdm =
      Multimaps.synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());

  final Map<String, TurtleWriter> files = newHashMap();
  TurtleWriter allCountries = null;

  final ConcurrentMap<String, String> adminsToIdsMap;
  private static final Logger logger = LoggerFactory.getLogger(GeonamesProducer.class);

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

    TurtleWriter writer = IoUtils.getWriter(output + "/altLabels.ttl");

    writer.startRDF();

    logger.info("Loading alt names ");
    var count = new AtomicInteger();
    try (Stream<String> lines = Files.lines(Paths.get(input_source, "alternateNames.txt"), UTF_8)) {
      lines
          .parallel()
          .forEach(
              l -> {
                logProgress(count, "altNames");
                String[] fields = l.split("\t");
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

                    Literal literal = lang.equals("") ? literal(label) : literal(label, lang);

                    Statement st =
                        statement(iri(codeToUri(code)), iri(property), literal(literal), null);
                    writer.handleStatement(st);

                  } else {
                    // postcodes
                  }
                }
              });
    }
    writer.endRDF();
    return this;
  }

  private String codeToUri(String code) {
    return NS_GEONAMES_INSTANCES + code + "/";
  }

  protected GeonamesProducer collectParents() throws Exception {

    logger.info("Loading parents");

    try (Stream<String> lines = Files.lines(Paths.get(input_source, "hierarchy.txt"), UTF_8)) {
      lines.forEach(
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
    }

    return this;
  }

  protected GeonamesProducer features() {
    logger.info("Parsing features");

    //     createDirsForContinents();

    var counter = new AtomicInteger(0);

    try (Stream<String> lines = Files.lines(Paths.get(input_source, "allCountries.txt"), UTF_8)) {
      lines
          .parallel()
          .forEach(
              line -> {
                GeonamesFeature feature = new GeonamesFeature(line);
                logProgress(counter, "features");
                boolean isDescriptionOfCountry = feature.getFeatureCodeField().startsWith("A.PCLI");
                getStatements(feature)
                    .forEach(st -> write(feature.getCountry(), st, isDescriptionOfCountry));
              });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    logger.info("Finished conversion, flushing and closing output files");

    files.keySet().stream()
        .filter(files::containsKey)
        .forEach(country -> files.get(country).endRDF());

    if (allCountries != null) {
      allCountries.endRDF();
    }
    files
        .values()
        .forEach(
            w -> {
              try {
                w.getWriter().close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    return this;
  }

  protected GeonamesProducer populateCodes() throws IOException {

    LineIterator it = FileUtils.lineIterator(new File(input_source, "allCountries.txt"), "UTF-8");

    while (it.hasNext()) {
      String text = it.nextLine();
      GeonamesFeature feature = new GeonamesFeature(text);

      int adminsNumber =
          (int)
              Stream.of(
                      feature.getAdmin1Value(),
                      feature.getAdmin2Value(),
                      feature.getAdmin3Value(),
                      feature.getAdmin4Value())
                  .filter(StringUtils::isNotEmpty)
                  .count();

      if (adminsNumber == 1 && feature.getFeatureCodeField().equals("A.PCLI")) {
        adminsToIdsMap.put(feature.getCountry() + feature.getAdmin1Value(), feature.getId());
      }

      if (adminsNumber == 1 && feature.getFeatureCodeField().equals("A.ADM1")) {
        adminsToIdsMap.put(feature.getCountry() + feature.getAdmin1Value(), feature.getId());
      }
      if (adminsNumber == 2 && feature.getFeatureCodeField().equals("A.ADM2")) {
        adminsToIdsMap.put(
            feature.getCountry() + feature.getAdmin1Value() + feature.getAdmin2Value(),
            feature.getId());
      }
      if (adminsNumber == 3 && feature.getFeatureCodeField().equals("A.ADM3")) {
        adminsToIdsMap.put(
            feature.getCountry()
                + feature.getAdmin1Value()
                + feature.getAdmin2Value()
                + feature.getAdmin3Value(),
            feature.getId());
      }
      if (adminsNumber == 4 && feature.getFeatureCodeField().equals("A.ADM4")) {
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

  private Collection<Statement> getStatements(GeonamesFeature feature) {

    Collection<Statement> statements = newArrayList();

    if (isNotEmpty(feature.getNameValue())) {
      Statement triple =
          statement(
              feature.getSubject(), iri(GN_ONTO + "name"), literal(feature.getNameValue()), null);
      statements.add(triple);
    }

    if (feature.getPopulationValue().length() > 1) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(GN_ONTO + "population"),
              literal(feature.getPopulationValue(), XSD.INTEGER),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getLongValue())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_WGS_SCHEMA + "long"),
              literal(feature.getLongValue(), XSD.DECIMAL),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getLatValue())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_WGS_SCHEMA + "lat"),
              literal(feature.getLatValue(), XSD.DECIMAL),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getAltValue())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_WGS_SCHEMA + "alt"),
              literal(feature.getAltValue(), XSD.DECIMAL),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getElevationValue())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_CUSTOM + "gtopo30"),
              literal(feature.getElevationValue(), XSD.DECIMAL),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getFeatureClassField())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(GN_ONTO + "featureClass"),
              iri(GN_ONTO + feature.getFeatureClassField()),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getFeatureCodeField())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(GN_ONTO + "featureCode"),
              iri(GN_ONTO + feature.getFeatureCodeField()),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getCountry())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(GN_ONTO + "countryCode"),
              literal(feature.getCountry()),
              null);
      statements.add(triple);
    }

    if (isNotEmpty(feature.getTimezoneValue())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_CUSTOM + "timezone"),
              literal(feature.getTimezoneValue()),
              null);
      statements.add(triple);
    }
    if (isNotEmpty(feature.getModificationDateValue())) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_DCTERMS + "modified"),
              literal(feature.getModificationDateValue(), XSD.DATE),
              null);
      statements.add(triple);
    }

    if (isNotEmpty(feature.getAdmin2Value()) && feature.getFeatureCodeField().equals("A.ADM2")) {
      Statement triple =
          statement(
              feature.getSubject(),
              iri(NS_CUSTOM + "admin2"),
              literal(feature.getAdmin2Value()),
              null);
      statements.add(triple);
    }

    if (links.containsKey(feature.getId())) {
      for (String link : links.get(feature.getId())) {
        String property =
            link.contains("wikipedia") ? GN_ONTO + "wikipediaArticle" : NS_FOAF + "page";
        Statement triple = statement(feature.getSubject(), iri(property), literal(link), null);
        statements.add(triple);
      }
      links.removeAll(feature.getId());
    }

    if (broaders.containsKey(feature.getId())) {
      for (String broaderCode : broaders.get(feature.getId())) {
        Statement triple =
            statement(
                feature.getSubject(),
                iri(GN_ONTO + "locatedIn"),
                iri(codeToUri(broaderCode)),
                null);
        statements.add(triple);
      }
      broaders.removeAll(feature.getId());
    }

    if (broadersAdm.containsKey(feature.getId())) {
      for (String broaderCode : broadersAdm.get(feature.getId())) {
        Statement triple =
            statement(
                feature.getSubject(),
                iri(GN_ONTO + "parentFeature"),
                iri(codeToUri(broaderCode)),
                null);
        statements.add(triple);
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
            statement(iri(feature.getUri()), iri(GN_ONTO + "parentFeature"), iri(father), null);
        statements.add(triple);
      }
    }
    return statements;
  }

  private void logProgress(AtomicInteger counter, String type) {
    counter.incrementAndGet();
    if (counter.get() % 100000 == 0) {
      logger.info(String.format("Processed %s %s", counter, type));
    }
  }

  public static void main(String... args) throws Exception {
    new GeonamesProducer("input_source", "output")
        .populateCodes()
        .collectParents()
        .labels()
        .features();
  }
}
