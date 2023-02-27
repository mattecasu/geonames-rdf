package convertion;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
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
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/*
 * inspired by https://github.com/europeana/tools/tree/master/trunk/annocultor/converters/geonames
 */

@FieldDefaults(level = PRIVATE)
@Accessors(chain = true)
public class GeonamesProducer {
    static final String NS_GEONAMES_INSTANCES = "http://sws.geonames.org/";
    static final String GN_ONTO = "http://www.geonames.org/ontology#";
    static final String NS_WGS_SCHEMA = "http://www.w3.org/2003/01/geo/wgs84_pos#";
    static final String NS_CUSTOM = "http://example.com/ontologies/customOntology#";
    static final String NS_DCTERMS = "http://purl.org/dc/terms/";
    static final String NS_FOAF = "http://xmlns.com/foaf/0.1/";

    // indexes
    static final int geonameid = 0; // id of record in geonames database
    static final int name = 1; // name
    // private static final int asciiname = 2; // name in plain ascii
    // public static int alternatenames = 3; // alternatenames, comma separated
    final int latitude = 4; // latitude (wgs84)
    static final int longitude = 5; // longitude (wgs84)
    static final int featureClass = 6;
    static final int featureCode = 7;
    static final int countryCode = 8; // ISO-3166 2-letter country code, 2 characters
    // private static final int cc2 = 9; // alternate country codes, comma separated, ISO-3166
    static final int admin1code = 10; // fipscode, see file admin1Codes.txt for names
    static final int admin2code = 11;
    static final int admin3code = 12;
    static final int admin4code = 13;
    static final int population = 14;
    static final int elevation = 15;
    static final int altitude = 16;
    static final int timezone = 17;
    static final int modificationDate = 18; // yyyy-MM-dd

    static final String input_source = "input_source";
    static final String output = "output";

    Multimap<String, String> links = ArrayListMultimap.create();
    Multimap<String, String> broaders = ArrayListMultimap.create();
    Multimap<String, String> broadersAdm = ArrayListMultimap.create();

    Map<String, TurtleWriter> files = newHashMap();
    TurtleWriter allCountries = null;

    ConcurrentMap<String, String> adminsToIdsMap;
    static final Logger logger = LoggerFactory.getLogger(GeonamesProducer.class);

    SimpleValueFactory factory = SimpleValueFactory.getInstance();

    public GeonamesProducer() {
        try {
            Files.createDirectory(Paths.get(output));
        } catch (IOException e) {
            e.printStackTrace();
        }
        DB db = DBMaker.fileDB("mapFile").make();
        adminsToIdsMap = db.hashMap("map", Serializer.STRING, Serializer.STRING).create();
        // namespaces = new ImmutableSet.Builder<Namespace>()
        // .add(new SimpleNamespace("gn", NS_GEONAMES_INSTANCES))
        // .add(new SimpleNamespace("gn_ont", NS_GEONAMES_ONTOLOGY))
        // .add(new SimpleNamespace("dcterms", DCTERMS.NAMESPACE))
        // .add(new SimpleNamespace("wgs84", NS_WGS_SCHEMA))
        // .add(new SimpleNamespace("xsd", XSD.NAMESPACE))
        // .add(new SimpleNamespace("custom", NS_CUSTOM))
        // .add(new SimpleNamespace("europeana", NS_EUROPEANA_SCHEMA))
        // .add(new SimpleNamespace("skos", SKOS.NAMESPACE))
        // .add(new SimpleNamespace("foaf", NS_FOAF))
        // .build();
    }

    private TurtleWriter getWriter(String path) {
        TurtleWriter writer = null;
        try {
            writer = new TurtleWriter(newOutputStream(Paths.get(output, "all-countries.ttl")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return writer;
    }

    private void write(String country, Statement triple, boolean isDescriptionOfCountry) {
        // String continent = countryToContinent.getProperty(country);
        if (isDescriptionOfCountry) {
            if (allCountries == null) {
                allCountries = getWriter(output + "all-countries.ttl");
                allCountries.startRDF();
            }
            allCountries.handleStatement(triple);
        }

        TurtleWriter writer = files.get(country);
        if (writer == null) {
            country = country.equals("") ? "noCountry" : country;
            writer = getWriter(output + "/" + country + ".ttl");
            files.put(country, writer);
            writer.startRDF();
        }
        writer.handleStatement(triple);
    }

    private GeonamesProducer labels() throws Exception {
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

                    Literal literal = lang.equals("") ? factory.createLiteral(label) : factory.createLiteral(label, lang);

                    Statement st = factory.createStatement(factory.createIRI(codeToUri(code)), factory.createIRI(property), literal);
                    writer.handleStatement(st);

                } else {
                    // postcodes
                }
            }
            count++;

            if (count % 100000 == 0) {
                logger.info("Passed " + count);
            }
        }
        it.close();
        writer.endRDF();
        return this;
    }

    private String codeToUri(String code) {
        return NS_GEONAMES_INSTANCES + code + "/";
    }

    private GeonamesProducer collectParents() throws Exception {

        logger.info("Loading parents");

        Stream<String> it = Files.lines(Paths.get(input_source, "hierarchy.txt"), Charset.forName("UTF-8"));

        it.forEach(line -> {
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

    private GeonamesProducer features() throws Exception {
        logger.info("Parsing features");

        // createDirsForContinents();

        final AtomicInteger counter = new AtomicInteger(0);
        Stream<String> it = Files.lines(Paths.get(input_source, "allCountries.txt"), Charset.forName("UTF-8"));

        it.forEach(line -> {

            String[] fields = line.split("\t");
            if (fields.length != 19) {
                it.close();
                throw new RuntimeException("::: Field names mismatch on " + line);
            }

            // progress
            counter.incrementAndGet();
            if (counter.get() % 100000 == 0) {
                logger.info("Passed " + counter);
            }
            String country = fields[countryCode];

            String id = fields[geonameid];
            String nameValue = fields[name];
            String uri = codeToUri(id);
            IRI subject = factory.createIRI(uri);
            String featureCodeField = fields[featureClass] + "." + fields[featureCode];
            String featureClassField = fields[featureClass];
            String populationValue = fields[population];
            String timezoneValue = fields[timezone];
            String modificationDateValue = fields[modificationDate];
            String latValue = fields[latitude];
            String longValue = fields[longitude];
            String altValue = fields[altitude];
            String elevationValue = fields[elevation];
            String admin1Value = fields[admin1code];
            String admin2Value = fields[admin2code];
            String admin3Value = fields[admin3code];
            String admin4Value = fields[admin4code];

            // if (includeRecordInConversion(featureCodeField, populationValue)) {

            boolean isDescriptionOfCountry = featureCodeField.startsWith("A.PCLI");

            if (isNotEmpty(nameValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(GN_ONTO + "name"),
                                factory.createLiteral(fields[name]));
                write(country, triple, isDescriptionOfCountry);
            }

            if (populationValue.length() > 1) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(GN_ONTO + "population"),
                                factory.createLiteral(populationValue, XSD.INTEGER));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(longValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_WGS_SCHEMA + "long"),
                                factory.createLiteral(longValue, XSD.DECIMAL));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(latValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_WGS_SCHEMA + "lat"),
                                factory.createLiteral(latValue, XSD.DECIMAL));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(altValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_WGS_SCHEMA + "alt"),
                                factory.createLiteral(altValue, XSD.DECIMAL));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(elevationValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_CUSTOM + "gtopo30"),
                                factory.createLiteral(elevationValue, XSD.DECIMAL));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(featureClassField)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(GN_ONTO + "featureClass"),
                                factory.createIRI(GN_ONTO + featureClassField));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(featureCodeField)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(GN_ONTO + "featureCode"),
                                factory.createIRI(GN_ONTO + featureCodeField));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(country)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(GN_ONTO + "countryCode"),
                                factory.createLiteral(country));
                write(country, triple, isDescriptionOfCountry);
            }

            if (isNotEmpty(timezoneValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_CUSTOM + "timezone"),
                                factory.createLiteral(timezoneValue));
                write(country, triple, isDescriptionOfCountry);
            }
            if (isNotEmpty(modificationDateValue)) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_DCTERMS + "modified"),
                                factory.createLiteral(modificationDateValue, XSD.DATE));
                write(country, triple, isDescriptionOfCountry);
            }

            if (isNotEmpty(admin2Value) && featureCodeField.equals("A.ADM2")) {
                Statement triple =
                        factory.createStatement(subject, factory.createIRI(NS_CUSTOM + "admin2"),
                                factory.createLiteral(admin2Value));
                write(country, triple, isDescriptionOfCountry);
            }

            if (links.containsKey(id)) {
                for (String link : links.get(id)) {
                    String property = link.contains("wikipedia") ? GN_ONTO + "wikipediaArticle" : NS_FOAF + "page";
                    Statement triple =
                            factory.createStatement(subject, factory.createIRI(property), factory.createLiteral(link));
                    write(country, triple, isDescriptionOfCountry);
                }
                links.removeAll(id);
            }

            if (broaders.containsKey(id)) {
                for (Object broaderCode : broaders.get(id)) {
                    String broader = (String) broaderCode;
                    Statement triple =
                            factory.createStatement(subject, factory.createIRI(GN_ONTO + "locatedIn"),
                                    factory.createIRI(codeToUri(broader)));
                    write(country, triple, isDescriptionOfCountry);
                }
                broaders.removeAll(id);
            }

            if (broadersAdm.containsKey(id)) {
                for (Object broaderCode : broadersAdm.get(id)) {
                    String broader = (String) broaderCode;
                    Statement triple =
                            factory.createStatement(subject, factory.createIRI(GN_ONTO + "parentFeature"),
                                    factory.createIRI(codeToUri(broader)));
                    write(country, triple, isDescriptionOfCountry);
                }
                broadersAdm.removeAll(id);
            }

            // fields admin1... admin4
            String denormalizedAdmins = country + admin1Value + admin2Value + admin3Value + admin4Value;

            if (adminsToIdsMap.containsKey(denormalizedAdmins)) {
                String father = codeToUri(adminsToIdsMap.get(denormalizedAdmins));

                if (!father.equals(uri)) {
                    Statement triple =
                            factory.createStatement(factory.createIRI(uri), factory.createIRI(GN_ONTO + "parentFeature"),
                                    factory.createIRI(father));
                    write(country, triple, isDescriptionOfCountry);
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

    private GeonamesProducer populateCodes() throws IOException {

        LineIterator it = FileUtils.lineIterator(new File(input_source, "allCountries.txt"), "UTF-8");

        while (it.hasNext()) {
            String text = it.nextLine();

            String[] fields = text.split("\t");

            String id = fields[geonameid];
            String featureCodeValue = fields[featureCode];
            String country = fields[countryCode];
            String admin1Value = fields[admin1code];
            String admin2Value = fields[admin2code];
            String admin3Value = fields[admin3code];
            String admin4Value = fields[admin4code];

            Long adminsNumber = Stream.of(admin1Value, admin2Value, admin3Value, admin4Value)
                    .filter(admin -> isNotEmpty(admin))
                    .count();

            if (adminsNumber.intValue() == 1 && featureCodeValue.equals("ADM1")) {
                adminsToIdsMap.put(country + admin1Value, id);
            }
            if (adminsNumber.intValue() == 2 && featureCodeValue.equals("ADM2")) {
                adminsToIdsMap.put(country + admin1Value + admin2Value, id);
            }
            if (adminsNumber.intValue() == 3 && featureCodeValue.equals("ADM3")) {
                adminsToIdsMap.put(country + admin1Value + admin2Value + admin3Value, id);
            }
            if (adminsNumber.intValue() == 4 && featureCodeValue.equals("ADM4")) {
                adminsToIdsMap.put(country + admin1Value + admin2Value + admin3Value + admin4Value, id);
            }
        }
        it.close();
        return this;
    }

    public static void main(String... args) throws Exception {
        new GeonamesProducer()
                .populateCodes()
                .collectParents()
                .labels()
                .features();
    }

}
