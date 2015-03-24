#!/bin/bash

#Copyright 2015 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
#
#This file is part of BigBWA.
#
#BigBWA is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#BigBWA is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with BigBWA. If not, see <http://www.gnu.org/licenses/>.

cp bwa-0.5.10-mt/BwaJni.java ./

( cd bwa-0.5.10-mt ; make ; make libbwa.so )
( cd bwa-0.5.10-mt ; zip -R bwa ./* )

cp bwa-0.5.10-mt/bwa.zip ./

JAR_FILES="./libs/commons-logging-1.1.3.jar:./libs/hadoop-common-2.6.0.jar:./libs/hadoop-mapreduce-client-core-2.6.0.jar"

javac -cp $JAR_FILES -Xlint:none *.java

jar cfe BigBWA.jar BigBWA ./*.class ./*.java
