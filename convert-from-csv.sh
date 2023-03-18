#!/usr/bin/env bash

./gradlew clean compileJava

INPUT_DIR="input_source"
if [ -d "${INPUT_DIR}" ]; then
  echo "Skipping download, ${INPUT_DIR} exists"
else
  echo "Downloading alternate names .."
  mkdir -p ${INPUT_DIR}
  cd ${INPUT_DIR}
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
fi

echo "Running conversion .."
./gradlew run -DmainClass=convertion.GeonamesProducer
