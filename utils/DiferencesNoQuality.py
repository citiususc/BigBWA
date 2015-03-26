#!/usr/bin/python
# -*- coding: utf-8 -*-

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

import sys
import os

import subprocess

#This program looks for the differences between two sam files. A difference happens only if one of the first eleven fields is different,
#and qualities are both bigger than 0. If only the qualities are different, it is not considered a difference.

def main():

	if(len(sys.argv)<3):
		print "Error!\n"
		print "Use: python DiferencesNoQuality.py Input1.sam Input2.sam > Diferences"
		return 0


	else: 
		nomeFicheiroEntrada1 = str(sys.argv[1])
		nomeFicheiroEntrada2 = str(sys.argv[2])


		ficheiroEntrada1 = open(nomeFicheiroEntrada1,'r')
		ficheiroEntrada2 = open(nomeFicheiroEntrada2,'r')
		
		
		lineNumber = 0
		
		linhaSaida1 = ""
		linhaSaida2 = ""
		
		line1 = ficheiroEntrada1.readline()
		line2 = ficheiroEntrada2.readline()
		
		numberOfDifferences = 0
		
		sameColumns = 11
		
		minQuality = 5
		
		while line1:
		
			diff = False
		
			if(not line1.startswith("@")):
				lineNumber = lineNumber + 1
		
			if line1 != line2:
				
				
				items1 = line1.split("\t")
				items2 = line2.split("\t")
				
				if((len(items1)<sameColumns) or (len(items2)<sameColumns)):
					print "--> "+line1.replace("\n","")
					print "<-- "+line2.replace("\n","")
					print "----------"
					
				else:
					for i in range(0,sameColumns):
						if(i!=4 and items1[i] != items2[i] and int(items1[4])>0 and int(items2[4])>0):
							diff = True
							
						
					if(diff):
						print "-> "+line1.replace("\n","")
						print "<- "+line2.replace("\n","")
						print "----------"
						numberOfDifferences = numberOfDifferences + 1
					

			
			line1 = ficheiroEntrada1.readline()
			line2 = ficheiroEntrada2.readline()
		
		ficheiroEntrada1.close()
		ficheiroEntrada2.close()

		print "Diffs = "+ str(numberOfDifferences)
		
		percentage = (float(numberOfDifferences) * 100.0) / float(lineNumber)
		
		print "Percentage of differences: "+str(percentage)
		
		return 1

	
			

if __name__ == '__main__':
	main()
