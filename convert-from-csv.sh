#!/usr/bin/env bash

mvn clean install
wget -O alternateNames.zip http://download.geonames.org/export/dump/alternateNames.zip
wget -O allCountries.zip http://download.geonames.org/export/dump/allCountries.zip
wget -O hierarchy.zip http://download.geonames.org/export/dump/hierarchy.zip
unzip alternateNames.zip && rm -f alternateNames.zip
unzip allCountries.zip && rm -f allCountries.zip
unzip hierarchy.zip && rm -f hierarchy.zip
mkdir -p input_source
mv -f allCountries.txt input_source/
mv -f alternateNames.txt input_source/ 
mv -f hierarchy.txt input_source/
mvn exec:exec -Dexec.args="-cp %classpath $MAVEN_OPTS GeonamesProducer"