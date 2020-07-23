# Changed some flags to the compiler to get it to work though
ifeq ($(ERL_EI_INCLUDE_DIR),)
$(warning ERL_EI_INCLUDE_DIR not set. Invoke via mix)
else
ERL_CFLAGS ?= -I$(ERL_EI_INCLUDE_DIR)
endif
ifeq ($(ERL_EI_LIBDIR),)
$(warning ERL_EI_LIBDIR not set. Invoke via mix)
else
ERL_LDFLAGS ?= -L$(ERL_EI_LIBDIR)
endif
ifeq ($(MIX_APP_PATH),)
$(warning MIX_APP_PATH not set. Invoke via mix)
endif
ifeq ($(MIX_DEPS_PATH),)
$(warning MIX_APP_PATH not set. Invoke via mix)
endif

ifeq ($(shell uname),Darwin)     # Mac OS X
PLATFORM_OPTIONS=-undefined dynamic_lookup
else
PLATFORM_OPTIONS=-Wl,-soneutron_nif $(CPP_PATH)/lib/libpulsar.so
endif

CPP_PATH=$(MIX_DEPS_PATH)/pulsar/pulsar-client-cpp

default_target: all

get_deps:
	@./build_deps.sh

all: get_deps priv priv/neutron_nif.so

priv:
	mkdir -p $(MIX_APP_PATH)/priv/

priv/neutron_nif.so: ./c_src/neutron_nif.c
	  $(CC) $^ -shared $(PLATFORM_OPTIONS) -fPIC -O3 -finline-functions -Wunused -Wall -Wpointer-arith -Wcast-align -Wcast-qual $(ERL_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -pedantic -L$(CPP_PATH)/lib -lpulsar -I$(CPP_PATH)/include -o $(MIX_APP_PATH)/priv/neutron_nif.so


clean:
	$(RM) $(MIX_APP_PATH)/priv/neutron_nif.so

.PHONY: default_target get_deps all clean