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

typedef struct {
    pulsar_message_id_t *msg_id;
} pulsar_msg_id;

typedef struct {
    pulsar_producer_t *producer;
    ErlNifPid callback_pid;
} pulsar_producer;

static ErlNifResourceType *nif_pulsar_client_type = NULL;
static ErlNifResourceType *nif_pulsar_consumer_type = NULL;
static ErlNifResourceType *nif_pulsar_msg_id_type = NULL;
static ErlNifResourceType *nif_pulsar_producer_type = NULL;

static void
destruct_pulsar_client(ErlNifEnv *env, void *arg)
{
    pulsar_client *p_client = arg;
    p_client->client = NULL;
    enif_free(p_client);
}

static void
destruct_pulsar_consumer(ErlNifEnv *env, void *arg)
{
    pulsar_consumer *p_consumer = arg;
    p_consumer->consumer = NULL;
    enif_free(p_consumer);
}

static void
destruct_pulsar_msg_id(ErlNifEnv *env, void *arg)
{
}

static void
destruct_pulsar_producer(ErlNifEnv *env, void *arg)
{
    pulsar_producer *p_producer = arg;
    p_producer->producer = NULL;
    enif_free(p_producer);
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
        return make_error_tuple(env, "couldn't retrieve client resource from given reference");
    }
    pulsar_client_close(p_client->client);
    pulsar_client_free(p_client->client);
    p_client->client = NULL;
    return make_atom(env, "ok");
}

static void listener_callback(pulsar_consumer_t* consumer, pulsar_message_t* message, void* ctx) {
    ErlNifPid actual_pid = *(ErlNifPid *)ctx;

    ErlNifBinary bin;
    ERL_NIF_TERM ret_bin;
    ErlNifEnv* env = enif_alloc_env();
    ERL_NIF_TERM temp = enif_make_string(env, pulsar_message_get_data(message), ERL_NIF_LATIN1);
    enif_inspect_iolist_as_binary(env, temp, &bin);
    ret_bin = enif_make_binary(env, &bin);

    pulsar_msg_id *p_msg_id;
    p_msg_id = enif_alloc_resource(nif_pulsar_msg_id_type, sizeof(pulsar_msg_id));

    p_msg_id->msg_id = NULL;
    p_msg_id->msg_id = pulsar_message_get_message_id(message);

    ERL_NIF_TERM p_msg_id_res = enif_make_resource(env, p_msg_id);
    enif_release_resource(p_msg_id);

    enif_send(NULL, &actual_pid, env, enif_make_tuple3(env, make_atom(env, "listener_callback"), ret_bin, p_msg_id_res));

    enif_free_env(env);
    pulsar_message_free(message);
}

ERL_NIF_TERM do_consume(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve client resource from given reference");
    }

    if(p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    ERL_NIF_TERM send_back_to_pid_atm = make_atom(env, "send_back_to_pid");
    ERL_NIF_TERM subscription_atm = make_atom(env, "subscription");
    ERL_NIF_TERM topic_atm = make_atom(env, "topic");
    ERL_NIF_TERM type_int_atm = make_atom(env, "type_int");

    ERL_NIF_TERM send_back_to_pid_term, topic_term, subscription_term, type_int_term;
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

    if (!enif_get_map_value(env, argv[1], type_int_atm, &type_int_term))
    {
        return make_error_tuple(env, "failed to make pulsar consumer type configuration");
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

    int consumer_type_int;
    if (!enif_get_int(env, type_int_term, &consumer_type_int))
    {
        return make_error_tuple(env, "failed to create consumer type from input type int");
    }
    pulsar_consumer_type consumer_type = (pulsar_consumer_type)consumer_type_int;

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
    pulsar_consumer_configuration_set_consumer_type(consumer_conf, consumer_type);
    pulsar_consumer_configuration_set_message_listener(consumer_conf, listener_callback, &p_consumer->callback_pid);

    pulsar_consumer_t *consumer;
    pulsar_result res = pulsar_client_subscribe(p_client->client, topic_str, subscription_str, consumer_conf, &consumer);
    if (res != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to make pulsar consumer");
    }
    pulsar_consumer_configuration_free(consumer_conf);
    p_consumer->consumer = consumer;

    ERL_NIF_TERM p_consumer_res = enif_make_resource(env, p_consumer);
    enif_release_resource(p_consumer);

    return enif_make_tuple2(env, make_atom(env, "ok"), p_consumer_res);
}

ERL_NIF_TERM ack(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }


    if(p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **) &p_msg_id))
    {
        return make_error_tuple(env, "couldn't retrieve msg_id resource from given reference");
    }

    if(p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    pulsar_result res = pulsar_consumer_acknowledge_id(p_consumer->consumer, p_msg_id->msg_id);

    if (res != pulsar_result_Ok) {
        pulsar_message_id_free(p_msg_id->msg_id);
        enif_release_resource(p_msg_id);
        return make_error_tuple(env, "failed to ack");
    } else {
        pulsar_message_id_free(p_msg_id->msg_id);
        enif_release_resource(p_msg_id);
        return make_atom(env, "ok");
    }
}

ERL_NIF_TERM nack(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }

    if(p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **) &p_msg_id))
    {
        return make_error_tuple(env, "couldn't retrieve msg_id resource from given reference");
    }

    if(p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    // this API doesn't nack on servers and only on client
    // this can lead to unbounded growth with nacked msg_ids
    // but there's no mechanism to prevent this
    pulsar_consumer_negative_acknowledge_id(p_consumer->consumer, p_msg_id->msg_id);

    return make_atom(env, "ok");
}

ERL_NIF_TERM destroy_consumer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }
    pulsar_consumer_close(p_consumer->consumer);
    pulsar_consumer_free(p_consumer->consumer);
    p_consumer->consumer = NULL;

    return make_atom(env, "ok");
}

ERL_NIF_TERM sync_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve client resource from given reference");
    }

    if(p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    ErlNifBinary topic_bin;

    int topic_ret = enif_inspect_binary(env, argv[1], &topic_bin);
    if (!topic_ret)
    {
        return make_error_tuple(env, "failed to create topic binary from input");
    }

    const char *topic_str = strndup((char*) topic_bin.data, topic_bin.size);

    ErlNifBinary msg_bin;

    int msg_ret = enif_inspect_binary(env, argv[2], &msg_bin);
    if (!msg_ret)
    {
        return make_error_tuple(env, "failed to create message binary from input");
    }

    const char *msg_str = strndup((char*) msg_bin.data, msg_bin.size);

    pulsar_producer_configuration_t* producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);

    pulsar_producer_t *producer;

    pulsar_result err = pulsar_client_create_producer(p_client->client, topic_str, producer_conf, &producer);

    pulsar_producer_configuration_free(producer_conf);

    if (err != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to make pulsar producer");
    }

    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, msg_str, strlen(msg_str));

    err = pulsar_producer_send(producer, message);

    if (err != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to send message");
    }

    pulsar_message_free(message);

    // Cleanup
    pulsar_producer_close(producer);
    pulsar_producer_free(producer);

    return make_atom(env, "ok");
}

static void delivery_callback(pulsar_result result, pulsar_message_id_t* msg_id, void* ctx) {
    ErlNifPid actual_pid = *(ErlNifPid *)ctx;
    ErlNifBinary bin;
    ERL_NIF_TERM ret_bin;
    ErlNifEnv* env = enif_alloc_env();
    ERL_NIF_TERM temp = enif_make_string(env, pulsar_message_id_str(msg_id), ERL_NIF_LATIN1);
    enif_inspect_iolist_as_binary(env, temp, &bin);
    ret_bin = enif_make_binary(env, &bin);

    if (result == pulsar_result_Ok) {
        enif_send(NULL, &actual_pid, env, enif_make_tuple3(env, make_atom(env, "delivery_callback"), make_atom(env, "ok"), ret_bin));
    } else {
        enif_send(NULL, &actual_pid, env, enif_make_tuple3(env, make_atom(env, "delivery_callback"), make_atom(env, "error"), ret_bin));
    }

    pulsar_message_id_free(msg_id);
    enif_free_env(env);
}

ERL_NIF_TERM create_async_producer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve client resource from given reference");
    }

    if(p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    ErlNifBinary topic_bin;

    int topic_ret = enif_inspect_binary(env, argv[1], &topic_bin);
    if (!topic_ret)
    {
        return make_error_tuple(env, "failed to create topic binary from input");
    }

    const char *topic_str = strndup((char*) topic_bin.data, topic_bin.size);

    ErlNifPid send_back_to_pid;
    if (!enif_get_local_pid(env, argv[2], &send_back_to_pid)) {
        return make_error_tuple(env, "failed to make pulsar producer callback pid");
    }

    // todo support more producer options
    pulsar_producer_configuration_t* producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);

    pulsar_producer_t *producer;

    pulsar_result err = pulsar_client_create_producer(p_client->client, topic_str, producer_conf, &producer);

    pulsar_producer_configuration_free(producer_conf);

    if (err != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to make pulsar producer");
    }

    pulsar_producer *p_producer;

    p_producer = enif_alloc_resource(nif_pulsar_producer_type, sizeof(pulsar_producer));
    if (!p_producer)
    {
        return make_error_tuple(env, "no_memory for creating pulsar producer");
    }

    p_producer->producer = NULL;
    p_producer->producer = producer;
    p_producer->callback_pid = send_back_to_pid;

    ERL_NIF_TERM p_producer_res = enif_make_resource(env, p_producer);
    enif_release_resource(p_producer);

    return enif_make_tuple2(env, make_atom(env, "ok"), p_producer_res);
}

ERL_NIF_TERM async_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_producer *p_producer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void **) &p_producer))
    {
        return make_error_tuple(env, "couldn't retrieve producer resource from given reference");
    }

    if(p_producer->producer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed producer");
    }

    ErlNifBinary msg_bin;

    int msg_ret = enif_inspect_binary(env, argv[1], &msg_bin);
    if (!msg_ret)
    {
        return make_error_tuple(env, "failed to create message binary from input");
    }

    const char *msg_str = strndup((char*) msg_bin.data, msg_bin.size);

    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, msg_str, strlen(msg_str));

    pulsar_producer_send_async(p_producer->producer, message, delivery_callback, &p_producer->callback_pid);

    pulsar_message_free(message);

    return make_atom(env, "ok");
}

ERL_NIF_TERM destroy_producer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_producer *p_producer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void **) &p_producer))
    {
        return make_error_tuple(env, "couldn't retrieve producer resource from given reference");
    }
    pulsar_producer_close(p_producer->producer);
    pulsar_producer_free(p_producer->producer);
    p_producer->producer = NULL;

    return make_atom(env, "ok");
}

/*
 * Below is used for nif lifecycle
 */
static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt_client;
    ErlNifResourceType *rt_consumer;
    ErlNifResourceType *rt_msg_id;
    ErlNifResourceType *rt_producer;

    rt_client = enif_open_resource_type(env, "neutron_nif", "pulsar_client", destruct_pulsar_client, ERL_NIF_RT_CREATE, NULL);
    if (!rt_client) return -1;

    rt_consumer = enif_open_resource_type(env, "neutron_nif", "pulsar_consumer", destruct_pulsar_consumer, ERL_NIF_RT_CREATE, NULL);
    if (!rt_consumer) return -1;

    rt_msg_id = enif_open_resource_type(env, "neutron_nif", "pulsar_msg_id", destruct_pulsar_msg_id, ERL_NIF_RT_CREATE, NULL);
    if (!rt_msg_id) return -1;

    rt_producer = enif_open_resource_type(env, "neutron_nif", "pulsar_producer", destruct_pulsar_producer, ERL_NIF_RT_CREATE, NULL);
    if (!rt_producer) return -1;

    nif_pulsar_client_type = rt_client;
    nif_pulsar_consumer_type = rt_consumer;
    nif_pulsar_msg_id_type = rt_msg_id;
    nif_pulsar_producer_type = rt_producer;

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
    {"sync_produce", 3, sync_produce, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"make_client", 1, make_client, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"destroy_client", 1, destroy_client, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"do_consume", 2, do_consume},
    {"ack", 2, ack, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nack", 2, nack, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"destroy_consumer", 1, destroy_consumer, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"create_async_producer", 3, create_async_producer, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"async_produce", 2, async_produce},
    {"destroy_producer", 1, destroy_producer, ERL_NIF_DIRTY_JOB_IO_BOUND},
};

ERL_NIF_INIT(Elixir.Neutron.PulsarNifs, nif_funcs, on_load, on_reload, on_upgrade, NULL)
