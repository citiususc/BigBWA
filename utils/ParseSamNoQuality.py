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


#this files takes as input a sam file generated from bwa and takes of optionals fields from the end and field number 5 (mapping quality)

def main():

	if(len(sys.argv)<2):
		print "Error!\n"
		print "Use: python ParseSamNoQuality.py Input.sam > ParsedFile.sam"
		return 0


	else: 
		nomeFicheiroEntrada1 = str(sys.argv[1])

		ficheiroEntrada1 = open(nomeFicheiroEntrada1,'r')
		
		
		lineNumber = 1
		
		linhaSaida1 = ""
		linhaSaida2 = ""
		
		line1 = ficheiroEntrada1.readline()
		
		sameColumns = 11
		
		while line1:	
				
			items1 = line1.split("\t")
				
			if(len(items1)>=sameColumns):
			
				string = ""
				
				for i in range(0,sameColumns):
					if(i!=4):
						string = string + items1[i]+"\t"
			
				print string
							
					
			lineNumber = lineNumber + 1
			
			line1 = ficheiroEntrada1.readline()
			
		
		ficheiroEntrada1.close()

		return 1

	
			

if __name__ == '__main__':
	main()
