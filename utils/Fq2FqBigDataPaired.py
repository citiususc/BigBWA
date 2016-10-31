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

#This program takes two fastq files and puts each paired read in a single line with its fields separated by <sep> and reads separated by <part>.

def main():

	if(len(sys.argv)<4):
		print "Erro\n"
		print "Uso correcto: python Fq2FqBigDataPaired [Input1.fq] [Input2.fq] [Output.fqBD]"
		return 0


	else: 
		nomeFicheiroEntrada1 = str(sys.argv[1])
		nomeFicheiroEntrada2 = str(sys.argv[2])
		nomeFicheiroSaida = str(sys.argv[3])


		ficheiroEntrada1 = open(nomeFicheiroEntrada1,'r')
		ficheiroEntrada2 = open(nomeFicheiroEntrada2,'r')
		ficheiroSaida = open(nomeFicheiroSaida,'w')
		
		
		i = 1
		
		linhaSaida1 = ""
		linhaSaida2 = ""
		
		line1 = ficheiroEntrada1.readline()
		line2 = ficheiroEntrada2.readline()
		
		while line1:
		
		#for line in ficheiroEntrada:

			
			
			if(i%4==0):
				
				linhaSaida1 = linhaSaida1+line1.replace("\n","")
				linhaSaida2 = linhaSaida2+line2.replace("\n","")
				ficheiroSaida.write(linhaSaida1+"<part>"+linhaSaida2+"\n")
				linhaSaida1 = ""
				linhaSaida2 = ""
				
			else:
				linhaSaida1 = linhaSaida1+line1.replace("\n","<sep>")
				linhaSaida2 = linhaSaida2+line2.replace("\n","<sep>")

			line1 = ficheiroEntrada1.readline()
			line2 = ficheiroEntrada2.readline()
			
			i = i + 1
		
		ficheiroEntrada1.close()
		ficheiroEntrada2.close()
		ficheiroSaida.close()		

		return 1

	
			

if __name__ == '__main__':
	main()
