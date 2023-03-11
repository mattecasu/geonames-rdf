#!/usr/bin/env bash

./gradlew clean compileJava
echo "Downloading alternate names .."
mkdir -p input_source
cd input_source
wget -q -O alternateNames.zip http://download.geonames.org/export/dump/alternateNames.zip
echo "Downloading all countries file .."
wget -q -O allCountries.zip http://download.geonames.org/export/dump/allCountries.zip
echo "Downloading hierarchies file .."
wget -q -O hierarchy.zip http://download.geonames.org/export/dump/hierarchy.zip
echo "Unzipping .."
unzip -q alternateNames.zip && rm -f alternateNames.zip
unzip -q allCountries.zip && rm -f allCountries.zip
unzip -q hierarchy.zip && rm -f hierarchy.zip
cd ..
echo "Running conversion .."
./gradlew run -DmainClass=convertion.GeonamesProducer
