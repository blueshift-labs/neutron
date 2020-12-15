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
PULSAR_VERSION=2.7.0

ifeq ($(shell uname),Darwin)     # Mac OS X
	PLATFORM_OPTIONS=-undefined dynamic_lookup -L/usr/local/Cellar/libpulsar/$(PULSAR_VERSION)/lib -lpulsar -rpath /usr/local/Cellar/libpulsar/$(PULSAR_VERSION)/lib
	# PLATFORM_OPTIONS=/usr/local/Cellar/libpulsar/$(PULSAR_VERSION)/lib/libpulsar.dylib
else
	PLATFORM_OPTIONS=/usr/lib/libpulsar.so
endif

default_target: all

all: get_deps priv priv/neutron_nif.so

get_deps:
	@./build_deps.sh

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
		$(PLATFORM_OPTIONS) \
		-I/usr/local/ssl/include \
		$(ERL_CFLAGS) \
		$(ERTS_CFLAGS) \
		$(ERL_LDFLAGS)

clean:
	$(RM) $(MIX_APP_PATH)/priv/neutron_nif.so

.PHONY: get_deps default_target all clean
