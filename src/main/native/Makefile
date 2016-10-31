include ./Makefile.common

.PHONY: bigbwa libbwa.so bwa clean

all: libbwa.so
	@echo "================================================================================"
	@echo "BigBWA library has been built."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "================================================================================"

bwa:
	$(MAKE) -C $(BWA_DIR)
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	cp $(BWA_DIR)/*.o $(BUILD_DIR)

bigbwa:
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	$(CC) $(BIGBWA_FLAGS) bwa_jni.c -o $(BUILD_DIR)/bwa_jni.o $(LIBBWA_LIBS)

libbwa.so: bigbwa bwa
	$(CC) $(LIBBWA_FLAGS) $(BUILD_DIR)/libbwa.so $(BUILD_DIR)/*.o $(LIBBWA_LIBS)
	cp $(BUILD_DIR)/libbwa.so ../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR)
	$(MAKE) clean -C $(BWA_DIR)
