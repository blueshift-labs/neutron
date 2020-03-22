#include "erl_nif.h"

#include <pulsar/c/client.h>

#include <stdio.h>
#include <string.h>

static ERL_NIF_TERM
make_atom(ErlNifEnv *env, const char *atom_name)
{
    ERL_NIF_TERM atom;

    if (enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1))
       return atom;

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv *env, const char *reason)
{
    ErlNifBinary bin;
    ERL_NIF_TERM temp = enif_make_string(env, reason, ERL_NIF_LATIN1);
    enif_inspect_iolist_as_binary(env, temp, &bin);
    enif_make_binary(env, &bin);
    return enif_make_tuple2(env, make_atom(env, "error"), enif_make_binary(env, &bin));
}

ERL_NIF_TERM test(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;

    int ret = enif_inspect_binary(env, argv[0], &bin);
    if (!ret)
    {
        return make_error_tuple(env, "failed to create binary from input");
    }

    const char *eval_str = strndup((char*) bin.data, bin.size);

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create("pulsar://localhost:6650", conf);

    pulsar_producer_configuration_t* producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);
    pulsar_producer_t *producer;

    pulsar_result err = pulsar_client_create_producer(client, "my-topic", producer_conf, &producer);

    const char* data = "my-content";
    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, data, strlen(data));

    err = pulsar_producer_send(producer, message);

    pulsar_message_free(message);

    // Cleanup
    pulsar_producer_close(producer);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);

    pulsar_client_close(client);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);

    return enif_make_tuple1(env, make_atom(env, "ok"));
}

/*
 * Below is used for nif lifecycle
 */
static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    return 0;
}

static int on_reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static int on_upgrade(ErlNifEnv* env, void** priv, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

// ToDo make a destroy that takes in resource and calls `enif_release_resource`
ErlNifFunc nif_funcs[] =
{
    {"test", 1, test, ERL_NIF_DIRTY_JOB_IO_BOUND},
};

ERL_NIF_INIT(Elixir.Neutron, nif_funcs, on_load, on_reload, on_upgrade, NULL)
