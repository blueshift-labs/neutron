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
ifeq ($(PULSAR_CLIENT_DIR),)
$(warning PULSAR_CLIENT_DIR not set. Invoke via mix)
endif
ifeq ($(PRIV_DIR),)
$(warning PRIV_DIR not set. Invoke via mix)
endif

default_target: all

get_deps:
	@./build_deps.sh

all: get_deps priv priv/neutron_nif.so

priv:
	mkdir -p priv

priv/neutron_nif.so: ./c_src/neutron_nif.c
	ls /app/deps/neutron/deps/pulsar/pulsar-client-cpp
	ls $(PULSAR_CLIENT_DIR)/lib
	ls $(PULSAR_CLIENT_DIR)/include
	ls /usr/local/lib/erlang/usr/lib

	$(CC) $^ -static -fPIC -O3 -DDEBUG -Wunused -Wall -Wpointer-arith -Wcast-align -Wcast-qual $(ERL_CFLAGS) $(ERL_LDFLAGS) -dynamiclib -undefined dynamic_lookup -pedantic -L$(PULSAR_CLIENT_DIR)/lib -lpulsar -I$(PULSAR_CLIENT_DIR)/include -o $(PRIV_DIR)

clean:
	$(RM) priv/neutron_nif.so

.PHONY: default_target get_deps all clean