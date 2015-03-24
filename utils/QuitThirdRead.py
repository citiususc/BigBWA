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

#Sometimes bwa takes out 3 alignments for a paired read. With this program the user can quit the third (and fourth and fiveth ...) read from the output file.

def main():

	if(len(sys.argv)<2):
		print "Error!\n"
		print "Use: python QuitThirdRead.py Input.sam > Output.sam"
		return 0


	else: 
		nomeFicheiroEntrada1 = str(sys.argv[1])

		ficheiroEntrada1 = open(nomeFicheiroEntrada1,'r')

		lineNumber = 1
		
		linhaSaida1 = ""
		
		line = ficheiroEntrada1.readline()
		
		numberOfDifferences = 0
		
		sameColumns = 11
		
		minQuality = 5
		
		while line:
		
			if(line.startswith("@")):
				print line.replace("\n","")
				line = ficheiroEntrada1.readline()
				
			else:
			
				diff = False
		
				items1 = line.split("\t")
			
				print line.replace("\n","")
			
				line = ficheiroEntrada1.readline()
			
				items2 = line.split("\t")
			
				print line.replace("\n","")
			
				line = ficheiroEntrada1.readline()		
				items1 = line.split("\t")
			
				while(items1[0] == items2[0]):
					line = ficheiroEntrada1.readline()		
					items1 = line.split("\t")
			
		
		ficheiroEntrada1.close()


		return 1

	
			

if __name__ == '__main__':
	main()
