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

#This program takes a fastq file and puts each read in a single line with its fields separated by <sep>.

def main():

	if(len(sys.argv)<3):
		print "Error!\n"
		print "Use: python Fq2FqBigData.py EntryFile.fastq OutputFile.fastqBD"
		return 0

	else: 
		nomeFicheiroEntrada = str(sys.argv[1])
		nomeFicheiroSaida = str(sys.argv[2])


		ficheiroEntrada = open(nomeFicheiroEntrada,'r')
		ficheiroSaida = open(nomeFicheiroSaida,'w')
		
		
		i = 1
		
		linhaSaida = ""
		
		for line in ficheiroEntrada:

			
			
			if(i%4==0):
				
				linhaSaida = linhaSaida+line.replace("\n","")
				ficheiroSaida.write(linhaSaida+"\n")
				linhaSaida = ""
			else:
				linhaSaida = linhaSaida+line.replace("\n","<sep>")

			i = i + 1
		
		ficheiroEntrada.close()
		ficheiroSaida.close()		

		return 1

	
			

if __name__ == '__main__':
	main()
