package namespaces;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public class Namespaces {

  public static final String NS_GEONAMES_INSTANCES = "http://sws.geonames.org/";
  public static final String GN_ONTO = "http://www.geonames.org/ontology#";
  public static final String NS_WGS_SCHEMA = "http://www.w3.org/2003/01/geo/wgs84_pos#";
  public static final String NS_CUSTOM = "http://example.com/ontologies/customOntology#";
  public static final String NS_DCTERMS = "http://purl.org/dc/terms/";
  public static final String NS_FOAF = "http://xmlns.com/foaf/0.1/";
  public static final String NS_EUROPEANA_SCHEMA = "http://www.europeana.eu/resolve/ontology/";

  public static Set<Namespace> getNamespaces() {
    return new ImmutableSet.Builder<Namespace>()
        .add(new SimpleNamespace("gn", NS_GEONAMES_INSTANCES))
        .add(new SimpleNamespace("gn_ont", GN_ONTO))
        .add(new SimpleNamespace("dcterms", DCTERMS.NAMESPACE))
        .add(new SimpleNamespace("wgs84", NS_WGS_SCHEMA))
        .add(new SimpleNamespace("xsd", XSD.NAMESPACE))
        .add(new SimpleNamespace("custom", NS_CUSTOM))
        .add(new SimpleNamespace("europeana", NS_EUROPEANA_SCHEMA))
        .add(new SimpleNamespace("skos", SKOS.NAMESPACE))
        .add(new SimpleNamespace("foaf", NS_FOAF))
        .build();
  }
}
