ifeq ($(ERTS_INCLUDE_DIR),)
$(warning ERTS_INCLUDE_DIR not set. Invoke via mix)
else
ERTS_CFLAGS ?= -I$(ERTS_INCLUDE_DIR)
endif
ifeq ($(ERL_INTERFACE_INCLUDE_DIR),)
$(warning ERL_INTERFACE_INCLUDE_DIR not set. Invoke via mix)
else
ERL_CFLAGS ?= -I$(ERL_INTERFACE_INCLUDE_DIR)
endif
ifeq ($(ERL_INTERFACE_LIB_DIR),)
$(warning ERL_INTERFACE_LIB_DIR not set. Invoke via mix)
else
ERL_LDFLAGS ?= -L$(ERL_INTERFACE_LIB_DIR)
endif
ifeq ($(MIX_APP_PATH),)
$(warning MIX_APP_PATH not set. Invoke via mix)
endif
ifeq ($(MIX_DEPS_PATH),)
$(warning MIX_APP_PATH not set. Invoke via mix)
endif

CPP_PATH=$(MIX_DEPS_PATH)/pulsar/pulsar-client-cpp

ifeq ($(shell uname),Darwin)     # Mac OS X
PLATFORM_OPTIONS=-undefined dynamic_lookup -L$(CPP_PATH)/lib -lpulsar -rpath $(CPP_PATH)/lib
else
PLATFORM_OPTIONS=-lssl -lcrypto -ldl -lpthread -lboost_system -lboost_regex -lcurl -lprotobuf -lzstd -lz $(CPP_PATH)/lib/libpulsar.a
endif

default_target: all

all: priv priv/neutron_nif.so

priv:
	mkdir -p $(MIX_APP_PATH)/priv/

priv/neutron_nif.so: ./c_src/neutron_nif.c
	$(CC) $^ \
		-shared \
		-pedantic \
		-fPIC \
		-O3 \
		-finline-functions \
		-o $(MIX_APP_PATH)/priv/neutron_nif.so \
		/usr/lib/libpulsar.so \
		-I/usr/local/ssl/include \
		$(ERL_CFLAGS) \
		$(ERTS_CFLAGS) \
		$(ERL_LDFLAGS)
clean:
	$(RM) $(MIX_APP_PATH)/priv/neutron_nif.so

.PHONY: default_target all clean
