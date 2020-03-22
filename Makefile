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

CPP_PATH=./deps/pulsar/pulsar-client-cpp

default_target: all

get_deps:
	@./build_deps.sh

all: get_deps priv priv/neutron_nif.so

priv:
	mkdir -p priv

priv/neutron_nif.so: ./c_src/neutron_nif.c
	$(CC) $^ -static -fPIC -O3 -DDEBUG -Wunused $(ERL_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -undefined dynamic_lookup -pedantic -L$(CPP_PATH)/lib -lpulsar -I$(CPP_PATH)/include -o $@
# 	$(CC) $^ -static -fPIC -O3 -DDEBUG -Wunused $(ERL_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -undefined dynamic_lookup -pedantic -o $@
# 	$(CC) $CPP_PATH/lib/libpulsar.dylib $CPP_PATH/lib/libpulsar.2.5.0.dylib -C ./c_src/ -fPIC -O3 -DDEBUG -g -Wunused $(ERL_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -undefined dynamic_lookup -pedantic -l:$CPP_PATH/lib/libpulsar.a -I $CPP_PATH/include -llibpulsar -llibpulsar.2.5.0 -o $@

clean:
	$(RM) priv/neutron_nif.so

.PHONY: default_target get_deps all clean