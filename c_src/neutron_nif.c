#include <erl_nif.h>

#include <pulsar/c/client.h>

#include <stdio.h>
#include <string.h>

typedef struct {
    pulsar_client_t *client;
} pulsar_client;

typedef struct {
    pulsar_consumer_t *consumer;
    ErlNifPid callback_pid;
} pulsar_consumer;

static ErlNifResourceType *nif_pulsar_client_type = NULL;
static ErlNifResourceType *nif_pulsar_consumer_type = NULL;

static void
destruct_pulsar_client(ErlNifEnv *env, void *arg)
{
    pulsar_client *pulsar_client = arg;
    pulsar_client->client = NULL;
    enif_free(pulsar_client);
}

static void
destruct_pulsar_consumer(ErlNifEnv *env, void *arg)
{
    pulsar_consumer *pulsar_consumer = arg;
    pulsar_consumer->consumer = NULL;
    enif_free(pulsar_consumer);
}

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

ERL_NIF_TERM make_client(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM atm_url = make_atom(env, "url");
    ERL_NIF_TERM atm_io_threads = make_atom(env, "io_threads");
    ERL_NIF_TERM atm_msg_listener_threads = make_atom(env, "msg_listener_threads");

    ERL_NIF_TERM url, io_threads, msg_listener_threads;
    if (!enif_get_map_value(env, argv[0], atm_url, &url))
    {
        return make_error_tuple(env, "failed to make pulsar client url configuration");
    }

    if (!enif_get_map_value(env, argv[0], atm_io_threads, &io_threads))
    {
        return make_error_tuple(env, "failed to make pulsar client io_threads configuration");
    }

    if (!enif_get_map_value(env, argv[0], atm_msg_listener_threads, &msg_listener_threads))
    {
        return make_error_tuple(env, "failed to make pulsar client msg_listener_threads configuration");
    }

    ErlNifBinary bin;

    int ret = enif_inspect_binary(env, url, &bin);
    if (!ret)
    {
        return make_error_tuple(env, "failed to create binary from input pulsar url");
    }

    const char *pulsar_str = strndup((char*) bin.data, bin.size);

    int io_threads_int;
    if (!enif_get_int(env, io_threads, &io_threads_int)) {
        return make_error_tuple(env, "failed to make pulsar client io_threads");
    }

    int msg_listener_threads_int;
    if (!enif_get_int(env, msg_listener_threads, &msg_listener_threads_int)) {
        return make_error_tuple(env, "failed to make pulsar client msg_listener_threads");
    }

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_configuration_set_io_threads(conf, io_threads_int);
    pulsar_client_configuration_set_message_listener_threads(conf, msg_listener_threads_int);

    pulsar_client_t *client = pulsar_client_create(pulsar_str, conf);

    pulsar_client_configuration_free(conf);

    pulsar_client *p_client;

    p_client = enif_alloc_resource(nif_pulsar_client_type, sizeof(pulsar_client));
    if (!p_client)
    {
        return make_error_tuple(env, "no_memory for creating pulsar client");
    }

    p_client->client = NULL;
    p_client->client = client;

    ERL_NIF_TERM p_client_res = enif_make_resource(env, p_client);
    enif_release_resource(p_client);
    return enif_make_tuple2(env, make_atom(env, "ok"), p_client_res);
}

ERL_NIF_TERM destroy_client(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve resource from given argument");
    }
    pulsar_client_close(p_client->client);
    pulsar_client_free(p_client->client);
    p_client->client = NULL;
    return enif_make_tuple1(env, make_atom(env, "ok"));
}

static void listener_callback(pulsar_consumer_t* consumer, pulsar_message_t* message, void* ctx) {
    ErlNifPid actual_pid = *(ErlNifPid *)ctx;
    ErlNifBinary bin;
    ERL_NIF_TERM ret_bin;
    ErlNifEnv* env = enif_alloc_env();
    ERL_NIF_TERM temp = enif_make_string(env, pulsar_message_get_data(message), ERL_NIF_LATIN1);
    enif_inspect_iolist_as_binary(env, temp, &bin);
    ret_bin = enif_make_binary(env, &bin);
    ERL_NIF_TERM int_term = enif_make_int64(env, pulsar_message_get_message_id(message));

    enif_send(NULL, &actual_pid, env, enif_make_tuple3(env, make_atom(env, "listener_callback"), ret_bin, int_term));

    enif_free_env(env);

    // todo remove the below and do seperate function for them
    pulsar_consumer_acknowledge(consumer, message);
    pulsar_message_free(message);
}

ERL_NIF_TERM do_consume(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve resource from given argument");
    }

    if(p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }


    ERL_NIF_TERM send_back_to_pid_atm = make_atom(env, "send_back_to_pid");
    ERL_NIF_TERM subscription_atm = make_atom(env, "subscription");
    ERL_NIF_TERM topic_atm = make_atom(env, "topic");

    ERL_NIF_TERM send_back_to_pid_term, topic_term, subscription_term;
    if (!enif_get_map_value(env, argv[1], send_back_to_pid_atm, &send_back_to_pid_term))
    {
        return make_error_tuple(env, "failed to make pulsar consumer send_back_to_pid configuration");
    }

    if (!enif_get_map_value(env, argv[1], subscription_atm, &subscription_term))
    {
        return make_error_tuple(env, "failed to make pulsar consumer subscription configuration");
    }

    if (!enif_get_map_value(env, argv[1], topic_atm, &topic_term))
    {
        return make_error_tuple(env, "failed to make pulsar consumer topic configuration");
    }

    ErlNifPid send_back_to_pid;
    if (!enif_get_local_pid(env, send_back_to_pid_term, &send_back_to_pid)) {
        return make_error_tuple(env, "failed to make pulsar consumer send_back_to_pid");
    }

    ErlNifBinary sub_bin;
    int ret_sub = enif_inspect_binary(env, subscription_term, &sub_bin);
    if (!ret_sub)
    {
        return make_error_tuple(env, "failed to create binary from input pulsar subscription");
    }

    const char *subscription_str = strndup((char*) sub_bin.data, sub_bin.size);

    ErlNifBinary bin_topic;
    int ret_topic = enif_inspect_binary(env, topic_term, &bin_topic);
    if (!ret_topic)
    {
        return make_error_tuple(env, "failed to create binary from input pulsar topic");
    }

    const char *topic_str = strndup((char*) bin_topic.data, bin_topic.size);


    pulsar_consumer *p_consumer;

    p_consumer = enif_alloc_resource(nif_pulsar_consumer_type, sizeof(pulsar_consumer));
    if (!p_consumer)
    {
        return make_error_tuple(env, "no_memory for creating pulsar consumer");
    }

    p_consumer->consumer = NULL;
    p_consumer->callback_pid = send_back_to_pid;

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    // ToDo make below configurable right now it just uses shared subscription
    pulsar_consumer_configuration_set_consumer_type(consumer_conf, pulsar_ConsumerShared);
    pulsar_consumer_configuration_set_message_listener(consumer_conf, listener_callback, &p_consumer->callback_pid);

    pulsar_consumer_t *consumer;
    pulsar_result res = pulsar_client_subscribe(p_client->client, topic_str, subscription_str, consumer_conf, &consumer);
    if (res != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to make pulsar consumer\n");
    }
    pulsar_consumer_configuration_free(consumer_conf);
    p_consumer->consumer = consumer;

    ERL_NIF_TERM p_consumer_res = enif_make_resource(env, p_consumer);
    enif_release_resource(p_consumer);

    return enif_make_tuple2(env, make_atom(env, "ok"), p_consumer_res);
}

ERL_NIF_TERM test_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve resource from given argument");
    }

    if(p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    pulsar_producer_configuration_t* producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);

    pulsar_producer_t *producer;

    pulsar_result err = pulsar_client_create_producer(p_client->client, "my-topic", producer_conf, &producer);

    pulsar_producer_configuration_free(producer_conf);

    if (err != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to make pulsar producer");
    }

    const char* data = "my-content";
    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, data, strlen(data));

    err = pulsar_producer_send(producer, message);

    if (err != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to send message");
    }

    pulsar_message_free(message);

    // Cleanup
    pulsar_producer_close(producer);
    pulsar_producer_free(producer);

    return enif_make_tuple1(env, make_atom(env, "ok"));
}

/*
 * Below is used for nif lifecycle
 */
static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt_client;
    ErlNifResourceType *rt_consumer;

    rt_client = enif_open_resource_type(env, "neutron_nif", "pulsar_client", destruct_pulsar_client, ERL_NIF_RT_CREATE, NULL);
    if (!rt_client) return -1;

    rt_consumer = enif_open_resource_type(env, "neutron_nif", "pulsar_consumer", destruct_pulsar_consumer, ERL_NIF_RT_CREATE, NULL);
    if (!rt_consumer) return -1;

    nif_pulsar_client_type = rt_client;
    nif_pulsar_consumer_type = rt_consumer;

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

ErlNifFunc nif_funcs[] =
{
    {"test_produce", 1, test_produce, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"make_client", 1, make_client, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"destroy_client", 1, destroy_client, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"do_consume", 2, do_consume},
};

ERL_NIF_INIT(Elixir.Neutron, nif_funcs, on_load, on_reload, on_upgrade, NULL)
