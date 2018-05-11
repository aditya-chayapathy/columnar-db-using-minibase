#!/usr/bin/env bash

cd ~/Desktop/PhaseII/Code/CSE510_DBMSI/javaminibase/

mkdir -p outdemo
javac  src/**/*.java -Xlint:unchecked -d outdemo
cd outdemo
cp ~/Desktop/sampledata1024.txt .
cp ~/Desktop/sampledata.txt .
