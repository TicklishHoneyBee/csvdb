include Makefile.inc

all: default

default: lib$(TARGET) $(TARGET)

$(TARGET): $(CLIENT_OBJS)
	$(CD) $(BUILD_DIR); $(CC) -o ../$(TARGET) $(CLIENT_OBJS) $(CLIBS)

lib$(TARGET): $(LIB_OBJS)
	$(CD) $(BUILD_DIR); $(CC) -o ../lib$(TARGET).so $(LIB_OBJS) $(LCLIBS)

distclean:
	$(RM) -f $(BUILD_DIR)/*.o

clean: distclean
	$(RM) -f $(TARGET)*
	$(RM) -f lib$(TARGET)*

install-client:
	install $(TARGET) $(BIN_DIR)/$(TARGET)
	-install man/$(TARGET).1 $(MAN_DIR)/$(TARGET).1


install-lib:
	install lib$(TARGET).so $(LIB_DIR)/lib$(TARGET).so

install: install-client install-lib

uninstall-client:
	$(RM) $(BIN_DIR)/$(TARGET)

uninstall-lib:
	$(RM) $(LIB_DIR)/lib$(TARGET).so

uninstall: uninstall-client uninstall-lib

fresh: clean all

%.o: $(SRC_DIR)/client/%.c
	$(CC) $(CFLAGS) -o $(BUILD_DIR)/$@ -c $<

%.o: $(SRC_DIR)/lib/%.c
	$(CC) $(LCFLAGS) -o $(BUILD_DIR)/$@ -c $<


.PHONY: all default distclean clean fresh $(TARGET)
