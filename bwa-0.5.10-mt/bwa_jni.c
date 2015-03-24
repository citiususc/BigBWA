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

#include "BwaJni.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include "main.h"
#include "utils.h"

#ifndef PACKAGE_VERS
# define PACKAGE_VERS 0.5.10-tpx
#endif

// -------------------

#define mkstr(s) #s
#define mkxstr(s) mkstr(s)
#define PACKAGE_VERSION mkxstr(PACKAGE_VERS)
#ifndef BLDDATE
# define BLDDATE unknown
#endif
#ifndef SVNURL
# define SVNURL unknown
#endif
#ifndef SVNREV
# define SVNREV unknown
#endif


// -------------------

static int usage()
{
	fprintf(stderr, "\n");
	fprintf(stderr, "Program: bwa (alignment via Burrows-Wheeler transformation)\n");
	fprintf(stderr, "Version: %s\n", PACKAGE_VERSION);
	fprintf(stderr, "Contact: Heng Li <lh3@sanger.ac.uk>\n\n");
	fprintf(stderr, "Usage:   bwa <command> [options]\n\n");
	fprintf(stderr, "Command: index         index sequences in the FASTA format\n");
	fprintf(stderr, "         aln           gapped/ungapped alignment\n");
	fprintf(stderr, "         samse         generate alignment (single ended)\n");
	fprintf(stderr, "         sampe         generate alignment (paired ended)\n");
	fprintf(stderr, "         bwasw         BWA-SW for long queries\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "         fa2pac        convert FASTA to PAC format\n");
	fprintf(stderr, "         pac2bwt       generate BWT from PAC\n");
	fprintf(stderr, "         pac2bwtgen    alternative algorithm for generating BWT\n");
	fprintf(stderr, "         bwtupdate     update .bwt to the new format\n");
	fprintf(stderr, "         pac_rev       generate reverse PAC\n");
	fprintf(stderr, "         bwt2sa        generate SA from BWT and Occ\n");
	fprintf(stderr, "         pac2cspac     convert PAC to color-space PAC\n");
	fprintf(stderr, "         stdsw         standard SW/NW alignment\n");
	fprintf(stderr, "\n");
	return 1;
}




JNIEXPORT jint JNICALL Java_BwaJni_bwa_1aln1_1jni (JNIEnv *env, jobject thisObj, jint argc, jobjectArray stringArray, jintArray lenStrings){

	//Parte argumentos
	char **argv;
	
	int stringCount = (*env)->GetArrayLength(env,stringArray);//env->GetArrayLength(stringArray);

	argv = (char **) malloc(stringCount*sizeof(char **));


	int i = 0;


    for (i=0; i<stringCount; i++) {
    	
        jstring string = (jstring) (*env)->GetObjectArrayElement(env, stringArray, i);

		//aux[i] = (*env)->GetStringUTFChars(env, string, 0);

        //argv[i] = (char *) malloc(inCArray[i]*sizeof(char));
        
        
        //strcpy(argv[i],aux[i]);
        
        
        argv[i] = (*env)->GetStringUTFChars(env, string, 0);
        //(*env)->ReleaseStringUTFChars(env, string, aux[i]);
        
        fprintf(stderr, "[%s] Arg %d '%s'\n",__func__,i, argv[i]);
        // Don't forget to call `ReleaseStringUTFChars` when you're done.
        //(*env)->ReleaseStringUTFChars(env, inJNIStr, inCStr);
        
    }

	//Fin parte argumentos
	
	
	char __attribute__((used)) svnid[] = mkxstr(@(#)$Id: bwa PACKAGE_VERS build-date: BLDDATE svn-url: SVNURL svn-rev: SVNREV $);

	time_t _prog_start = 1;
	char bwaversionstr[200] = { "" };
	char bwablddatestr[200] = { "" };

	
	int temp_stdout;
	temp_stdout = dup(fileno(stdout));
	
	if(temp_stdout == -1){
		fprintf(stderr, "[%s] Error saving stdout\n", __func__);
	}
	
	struct timeval st;
	int j;
	int ret;
	
	if (argc < 2) return usage();

	// -------------------

        gettimeofday(&st, NULL);
        _prog_start = st.tv_sec * 1000000L + (time_t)st.tv_usec;

        sprintf(bwaversionstr,"%s-%s",mkxstr(PACKAGE_VERS),mkxstr(SVNREV));
        sprintf(bwablddatestr,"%s",mkxstr(BLDDATE));

        for(j=1;j<argc;j++){
          if(strncmp(argv[j],"-ver",4) == 0){
            fprintf(stdout,"BWA program (%s)\n", bwaversionstr);
            return 0;
          }
        }

	// -------------------

	if (strcmp(argv[1], "aln") == 0) ret = bwa_aln(argc-1, argv+1);
	else {
		fprintf(stderr, "[main] unrecognized command '%s'\n", argv[1]);
		return 1;
	}
	
	fflush(stdout);
	fclose(stdout);

	FILE *fp2 = fdopen(temp_stdout, "w");
	
	stdout = fp2;
	
	for (i=0; i<stringCount; i++) {
    	
        (*env)->ReleaseStringUTFChars(env, (jstring) (*env)->GetObjectArrayElement(env, stringArray, i), argv[i]);
        
    }
	
	return ret;
	
	
}


JNIEXPORT jint JNICALL Java_BwaJni_bwa_1aln2_1jni (JNIEnv *env, jobject thisObj, jint argc, jobjectArray stringArray, jintArray lenStrings){

	//Parte argumentos
	char **argv;
	
	int stringCount = (*env)->GetArrayLength(env,stringArray);//env->GetArrayLength(stringArray);

	argv = (char **) malloc(stringCount*sizeof(char **));


	int i = 0;


    for (i=0; i<stringCount; i++) {
    	
        jstring string = (jstring) (*env)->GetObjectArrayElement(env, stringArray, i);

		//aux[i] = (*env)->GetStringUTFChars(env, string, 0);

        //argv[i] = (char *) malloc(inCArray[i]*sizeof(char));
        
        
        //strcpy(argv[i],aux[i]);
        
        
        argv[i] = (*env)->GetStringUTFChars(env, string, 0);
        //(*env)->ReleaseStringUTFChars(env, string, aux[i]);
        
        fprintf(stderr, "[%s] Arg %d '%s'\n",__func__,i, argv[i]);
        // Don't forget to call `ReleaseStringUTFChars` when you're done.
        //(*env)->ReleaseStringUTFChars(env, inJNIStr, inCStr);
        
    }

	//Fin parte argumentos
	
	char __attribute__((used)) svnid[] = mkxstr(@(#)$Id: bwa PACKAGE_VERS build-date: BLDDATE svn-url: SVNURL svn-rev: SVNREV $);

	time_t _prog_start = 1;
	char bwaversionstr[200] = { "" };
	char bwablddatestr[200] = { "" };

	
	
	
	
	int temp_stdout;
	temp_stdout = dup(fileno(stdout));
	
	if(temp_stdout == -1){
		fprintf(stderr, "[%s] Error saving stdout\n", __func__);
	}
	
	struct timeval st;
	int j;
	int ret;
	
	if (argc < 2) return usage();

	if(strcmp(argv[2],"-f") == 0){
		xreopen(argv[3], "wb", stdout);
		fprintf(stderr, "[%s] Exit redirected to: %s\n", __func__,argv[3]);
	}
	
	// -------------------

        gettimeofday(&st, NULL);
        _prog_start = st.tv_sec * 1000000L + (time_t)st.tv_usec;

        sprintf(bwaversionstr,"%s-%s",mkxstr(PACKAGE_VERS),mkxstr(SVNREV));
        sprintf(bwablddatestr,"%s",mkxstr(BLDDATE));

        for(j=1;j<argc;j++){
          if(strncmp(argv[j],"-ver",4) == 0){
            fprintf(stdout,"BWA program (%s)\n", bwaversionstr);
            return 0;
          }
        }

	// -------------------

	if (strcmp(argv[1], "aln") == 0) ret = bwa_aln(argc-1, argv+1);
	else {
		fprintf(stderr, "[main] unrecognized command '%s'\n", argv[1]);
		return 1;
	}

	fflush(stdout);
	fclose(stdout);

	FILE *fp2 = fdopen(temp_stdout, "w");
	
	stdout = fp2;
	
	for (i=0; i<stringCount; i++) {
    	
        (*env)->ReleaseStringUTFChars(env, (jstring) (*env)->GetObjectArrayElement(env, stringArray, i), argv[i]);
        
    }
	
	return ret;
	
}



JNIEXPORT jint JNICALL Java_BwaJni_bwa_1sampe_1jni (JNIEnv *env, jobject thisObj, jint argc, jobjectArray stringArray, jintArray lenStrings){

	//Parte argumentos
	char **argv;
	
	int stringCount = (*env)->GetArrayLength(env,stringArray);//env->GetArrayLength(stringArray);

	argv = (char **) malloc(stringCount*sizeof(char **));
	//aux = (char **) malloc(stringCount*sizeof(char **));


	int i = 0;


    for (i=0; i<stringCount; i++) {
    	
        jstring string = (jstring) (*env)->GetObjectArrayElement(env, stringArray, i);

		//aux[i] = (*env)->GetStringUTFChars(env, string, 0);

        //argv[i] = (char *) malloc(inCArray[i]*sizeof(char));
        
        
        //strcpy(argv[i],aux[i]);
        
        
        argv[i] = (*env)->GetStringUTFChars(env, string, 0);
        //(*env)->ReleaseStringUTFChars(env, string, aux[i]);
        
        fprintf(stderr, "[main] Arg %d '%s'\n",i, argv[i]);
        // Don't forget to call `ReleaseStringUTFChars` when you're done.
        //(*env)->ReleaseStringUTFChars(env, inJNIStr, inCStr);
        
    }

	//Fin parte argumentos

	char __attribute__((used)) svnid[] = mkxstr(@(#)$Id: bwa PACKAGE_VERS build-date: BLDDATE svn-url: SVNURL svn-rev: SVNREV $);

	time_t _prog_start = 1;
	char bwaversionstr[200] = { "" };
	char bwablddatestr[200] = { "" };

	

	struct timeval st;
	int j;
	int ret;
	
	if (argc < 2) return usage();
	
	if(strcmp(argv[2],"-f") == 0){
		xreopen(argv[3], "wb", stdout);
		fprintf(stderr, "[%s] Exit redirected to: %s\n", __func__,argv[3]);
	}
	// -------------------

        gettimeofday(&st, NULL);
        _prog_start = st.tv_sec * 1000000L + (time_t)st.tv_usec;

        sprintf(bwaversionstr,"%s-%s",mkxstr(PACKAGE_VERS),mkxstr(SVNREV));
        sprintf(bwablddatestr,"%s",mkxstr(BLDDATE));

        for(j=1;j<argc;j++){
          if(strncmp(argv[j],"-ver",4) == 0){
            fprintf(stdout,"BWA program (%s)\n", bwaversionstr);
            return 0;
          }
        }

	// -------------------

	if (strcmp(argv[1], "sampe") == 0) ret = bwa_sai2sam_pe(argc-1, argv+1);
	else {
		fprintf(stderr, "[main] unrecognized command '%s'\n", argv[1]);
		return 1;
	}
	
	fflush(stdout);
	fclose(stdout);
	
	for (i=0; i<stringCount; i++) {
    	
        (*env)->ReleaseStringUTFChars(env, (jstring) (*env)->GetObjectArrayElement(env, stringArray, i), argv[i]);
        
    }
	
	return ret;
}

