#!/usr/bin/env bash

./gradlew clean compileJava
echo "Downloading alternate names .."
wget -q -O alternateNames.zip http://download.geonames.org/export/dump/alternateNames.zip
echo "Downloading all countries file .."
wget -q -O allCountries.zip http://download.geonames.org/export/dump/allCountries.zip
echo "Downloading hierarchies file .."
wget -q -O hierarchy.zip http://download.geonames.org/export/dump/hierarchy.zip
echo "Unzipping .."
unzip -q alternateNames.zip && rm -f alternateNames.zip
unzip -q allCountries.zip && rm -f allCountries.zip
unzip -q hierarchy.zip && rm -f hierarchy.zip
mkdir -p input_source
mv -f allCountries.txt input_source/
mv -f alternateNames.txt input_source/
mv -f hierarchy.txt input_source/
echo "Running conversion .."
./gradlew run -DmainClass=convertion.GeonamesProducer
