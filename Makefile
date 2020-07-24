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

ifeq ($(shell uname),Darwin)     # Mac OS X
PLATFORM_OPTIONS=-undefined dynamic_lookup -L$(CPP_PATH)/lib -lpulsar
else
PLATFORM_OPTIONS=-lssl -lcrypto -ldl -lpthread -lboost_system -lboost_regex -lcurl -lprotobuf -lzstd -lz $(CPP_PATH)/lib/libpulsar.a
endif

CPP_PATH=$(MIX_DEPS_PATH)/pulsar/pulsar-client-cpp

default_target: all

get_deps:
	@./build_deps.sh

all: get_deps priv priv/neutron_nif.so

priv:
	mkdir -p $(MIX_APP_PATH)/priv/

priv/neutron_nif.so: ./c_src/neutron_nif.c
	  $(CC) $^ -shared -lei $(PLATFORM_OPTIONS) -fPIC -O3 -finline-functions -Wunused -Wall -Wpointer-arith -Wcast-align -Wcast-qual $(ERL_CFLAGS) $(ERTS_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -pedantic -I/usr/local/ssl/include -L/usr/local/ssl/lib -I$(CPP_PATH)/include -o $(MIX_APP_PATH)/priv/neutron_nif.so

clean:
	$(RM) $(MIX_APP_PATH)/priv/neutron_nif.so

.PHONY: default_target get_deps all clean