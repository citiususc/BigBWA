include ./Makefile.common

.PHONY: bigbwa libbwa.so bwa clean

all: bigbwa_java
	@echo "================================================================================"
	@echo "BigBWA has been compiled."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "HADOOP_HOME = $(DEBUG)"
	@echo "================================================================================"

bwa:
	$(MAKE) -C $(BWA_DIR)/$(BWA)/
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	cp $(BWA_DIR)/$(BWA)/*.o $(BUILD_DIR)/

bigbwa:
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	$(CC) $(BIGBWA_FLAGS) $(SRC_DIR)/bwa_jni.c -o $(BUILD_DIR)/bwa_jni.o -lrt

libbwa.so: bigbwa bwa
	$(CC) $(LIBBWA_FLAGS) $(BUILD_DIR)/libbwa.so $(BUILD_DIR)/*.o $(LIBBWA_LIBS)
	cd $(BUILD_DIR) && zip -r bwa ./* && cd ..

bigbwa_java: libbwa.so
	$(JAVAC) -cp $(JAR_FILES) -d $(BUILD_DIR) -Xlint:none $(SRC_DIR)*.java
	cd $(BUILD_DIR) && $(JAR) cfe BigBWA.jar BigBWA ./*.class && cd ..
	cd $(BUILD_DIR) && $(JAR) cfe BigBWASeq.jar BwaSeq ./*.class && cd ..

clean:
	$(RMR) $(BUILD_DIR)
	$(MAKE) clean -C $(BWA_DIR)/$(BWA)
