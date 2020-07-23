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

ifeq ($(shell uname),Darwin)     # Mac OS X
PLATFORM_OPTIONS=-undefined dynamic_lookup
else
PLATFORM_OPTIONS=-Wl,-soneutron_nif $(CPP_PATH)/lib/libpulsar.so
endif

CPP_PATH=./deps/pulsar/pulsar-client-cpp

default_target: all

get_deps:
	@./build_deps.sh

all: get_deps priv priv/neutron_nif.so

priv:
	mkdir -p priv

priv/neutron_nif.so: ./c_src/neutron_nif.c
	  $(CC) $^ -shared $(PLATFORM_OPTIONS) -fPIC -O2 -Wunused -Wall -Wpointer-arith -Wcast-align -Wcast-qual $(ERL_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -pedantic -L$(CPP_PATH)/lib -lpulsar -I$(CPP_PATH)/include -o $@


clean:
	$(RM) priv/neutron_nif.so

.PHONY: default_target get_deps all clean