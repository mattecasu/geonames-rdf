package conversion;

public class GeonamesHeaders {

  public static final int geonameid = 0; // id of record in geonames database
  public static final int name = 1; // name
  // private static final int asciiname = 2; // name in plain ascii
  // public static int alternatenames = 3; // alternatenames, comma separated
  public static final int latitude = 4; // latitude (wgs84)
  public static final int longitude = 5; // longitude (wgs84)
  public static final int featureClass = 6;
  public static final int featureCode = 7;
  public static final int countryCode = 8; // ISO-3166 2-letter country code, 2 characters
  // private static final int cc2 = 9; // alternate country codes, comma separated, ISO-3166
  public static final int admin1code = 10; // fipscode, see file admin1Codes.txt for names
  public static final int admin2code = 11;
  public static final int admin3code = 12;
  public static final int admin4code = 13;
  public static final int population = 14;
  public static final int elevation = 15;
  public static final int altitude = 16;
  public static final int timezone = 17;
  public static final int modificationDate = 18; // yyyy-MM-dd
}
