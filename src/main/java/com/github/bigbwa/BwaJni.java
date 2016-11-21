/**
  * Copyright 2015 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
  * 
  * This file is part of BigBWA.
  *
  * BigBWA is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * BigBWA is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
  * GNU General Public License for more details.
  * 
  * You should have received a copy of the GNU General Public License
  * along with BigBWA. If not, see <http://www.gnu.org/licenses/>.
  */

package com.github.bigbwa;

import cz.adamh.utils.NativeUtils;

import java.io.IOException;

/**
 * Class that calls BWA functions by means of JNI
 *
 * @author José M. Abuín
 */
public class BwaJni {

	static {
		try {
			NativeUtils.loadLibraryFromJar("/libbwa.so");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private native int bwa_jni(int argc, String[] argv, int[] lenStrings);
	
	public static int Bwa_Jni(String[] args) {
		
		int[] lenStrings = new int[args.length];
		
		int i=0;
		
		for(String argumento: args){
			lenStrings[i] = argumento.length();
		}
		
		//new BwaJni().bwa_jni(args.length, args, lenStrings);
		int returnCode = new BwaJni().bwa_jni(args.length, args, lenStrings);

		return returnCode;
	}

}

