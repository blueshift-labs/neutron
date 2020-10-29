#include <erl_nif.h>

#include "pulsar/c/client.h"

#include <stdio.h>
#include <string.h>

typedef struct {
    ERL_NIF_TERM atomOk;
    ERL_NIF_TERM atomError;
    ERL_NIF_TERM atomDelivery;
    ERL_NIF_TERM atomNeutronMsg;
} atoms;

atoms ATOMS;

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

typedef struct {
    const char* msg_data;
    ErlNifPid callback_pid;
} delivery_callback_ctx;

static ErlNifResourceType *nif_pulsar_client_type = NULL;
static ErlNifResourceType *nif_pulsar_consumer_type = NULL;
static ErlNifResourceType *nif_pulsar_msg_id_type = NULL;
static ErlNifResourceType *nif_pulsar_producer_type = NULL;
static ErlNifResourceType *nif_delivery_callback_ctx_type = NULL;

static void msg_id_destr(ErlNifEnv *env, void *obj) {
  pulsar_msg_id *p_msg_id = (pulsar_msg_id *)obj;
  pulsar_message_id_free(p_msg_id->msg_id);
  p_msg_id->msg_id = NULL;
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
make_binary(ErlNifEnv* env, const char* buff, size_t length)
{
    ERL_NIF_TERM term;
    uint8_t *destination_buffer = enif_make_new_binary(env, length, &term);
    memcpy(destination_buffer, buff, length);
    return term;
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv* env, const char* error)
{
    return enif_make_tuple2(env, ATOMS.atomError, make_binary(env, error, strlen(error)));
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
    return enif_make_tuple2(env, ATOMS.atomOk, p_client_res);
}

ERL_NIF_TERM destroy_client(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve client resource from given reference");
    }

    if (p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }
    pulsar_client_close(p_client->client);
    pulsar_client_free(p_client->client);
    p_client->client = NULL;
    enif_release_resource(p_client);
    return ATOMS.atomOk;
}

static void listener_callback(pulsar_consumer_t* consumer, pulsar_message_t* message, void* ctx) {
    ErlNifPid actual_pid = *(ErlNifPid *)ctx;

    ErlNifEnv* env = enif_alloc_env();

    // id
    pulsar_msg_id *p_msg_id;
    p_msg_id = enif_alloc_resource(nif_pulsar_msg_id_type, sizeof(pulsar_msg_id));

    p_msg_id->msg_id = NULL;
    p_msg_id->msg_id = pulsar_message_get_message_id(message);

    ERL_NIF_TERM p_msg_id_res = enif_make_resource(env, p_msg_id);
    enif_release_resource(p_msg_id);

    // partition key
    const char *part_key = pulsar_message_get_partitionKey(message);
    ERL_NIF_TERM ret_part_key = make_binary(env, part_key, strlen(part_key));

    // ordering key
    const char *order_key = pulsar_message_get_orderingKey(message);
    ERL_NIF_TERM ret_order_key = make_binary(env, order_key, strlen(order_key));

    // publish ts
    uint64_t publish_ts = pulsar_message_get_publish_timestamp(message);
    ERL_NIF_TERM ret_publish_ts = enif_make_uint64(env, publish_ts);

    // event ts
    uint64_t event_ts = pulsar_message_get_event_timestamp(message);
    ERL_NIF_TERM ret_event_ts = enif_make_uint64(env, event_ts);

    // redeliver count
    int redeliver_ct = pulsar_message_get_redelivery_count(message);
    ERL_NIF_TERM ret_redeliver_ct = enif_make_int(env, redeliver_ct);

    // properties
    pulsar_string_map_t *props = pulsar_message_get_properties(message);
    int props_size = pulsar_string_map_size(props);
    ERL_NIF_TERM ret_props_arr[props_size];
    for(int i = 0; i < props_size; i++){
        const char *key = pulsar_string_map_get_key(props, i);
        const char *value = pulsar_string_map_get_value(props, i);
        ret_props_arr[i] = enif_make_tuple2(env,
            make_binary(env, key, strlen(key)),
            make_binary(env, value, strlen(value))
        );
    }
    ERL_NIF_TERM ret_props = enif_make_list_from_array(env, ret_props_arr, props_size);
    pulsar_string_map_free(props);

    // payload
    ERL_NIF_TERM ret_payload = make_binary(env, pulsar_message_get_data(message), pulsar_message_get_length(message));

    enif_send(NULL, &actual_pid, env, enif_make_tuple9(env, ATOMS.atomNeutronMsg,
        p_msg_id_res, ret_part_key, ret_order_key, ret_publish_ts, ret_event_ts, ret_redeliver_ct, ret_props, ret_payload));

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

    if (p_client->client == NULL)
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

    return enif_make_tuple2(env, ATOMS.atomOk, p_consumer_res);
}

ERL_NIF_TERM ack(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }

    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **) &p_msg_id))
    {
        return make_error_tuple(env, "couldn't retrieve msg_id resource from given reference");
    }

    if (p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    pulsar_result res = pulsar_consumer_acknowledge_id(p_consumer->consumer, p_msg_id->msg_id);

    if (res != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to ack");
    } else {
        return ATOMS.atomOk;
    }
}

ERL_NIF_TERM ack_all(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }


    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **) &p_msg_id))
    {
        return make_error_tuple(env, "couldn't retrieve msg_id resource from given reference");
    }

    if (p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    pulsar_result res = pulsar_consumer_acknowledge_cumulative_id(p_consumer->consumer, p_msg_id->msg_id);

    if (res != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to ack_all");
    } else {
        return ATOMS.atomOk;
    }
}

ERL_NIF_TERM nack(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }

    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **) &p_msg_id))
    {
        return make_error_tuple(env, "couldn't retrieve msg_id resource from given reference");
    }

    if (p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    // this API doesn't nack on servers and only on client
    // this can lead to unbounded growth with nacked msg_ids
    // but there's no mechanism to prevent this
    pulsar_consumer_negative_acknowledge_id(p_consumer->consumer, p_msg_id->msg_id);

    return ATOMS.atomOk;
}

ERL_NIF_TERM destroy_consumer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **) &p_consumer))
    {
        return make_error_tuple(env, "couldn't retrieve consumer resource from given reference");
    }

    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_consumer_close(p_consumer->consumer);
    pulsar_consumer_free(p_consumer->consumer);
    p_consumer->consumer = NULL;
    enif_release_resource(p_consumer);

    return ATOMS.atomOk;
}

void maybe_set_message_options(ErlNifEnv* env, pulsar_message_t *message, ERL_NIF_TERM map)
{
    ERL_NIF_TERM deliver_after_ms_atm = make_atom(env, "deliver_after_ms");
    ERL_NIF_TERM deliver_at_ms_atm = make_atom(env, "deliver_at_ms");

    unsigned long deliver_after_ms;
    ERL_NIF_TERM deliver_after_ms_term;
    if (enif_get_map_value(env, map, deliver_after_ms_atm, &deliver_after_ms_term) && enif_get_uint64(env, deliver_after_ms_term, &deliver_after_ms))
    {
        pulsar_message_set_deliver_after(message, (uint64_t)deliver_after_ms);
    }

    unsigned long deliver_at_ms;
    ERL_NIF_TERM deliver_at_ms_term;
    if (enif_get_map_value(env, map, deliver_at_ms_atm, &deliver_at_ms_term) && enif_get_uint64(env, deliver_at_ms_term, &deliver_at_ms))
    {
        pulsar_message_set_deliver_at(message, (uint64_t)deliver_at_ms);
    }
}

ERL_NIF_TERM sync_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **) &p_client))
    {
        return make_error_tuple(env, "couldn't retrieve client resource from given reference");
    }

    if (p_client->client == NULL)
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
    maybe_set_message_options(env, message, argv[3]);

    err = pulsar_producer_send(producer, message);

    if (err != pulsar_result_Ok) {
        return make_error_tuple(env, "failed to send message");
    }

    pulsar_message_free(message);

    // Cleanup
    pulsar_producer_close(producer);
    pulsar_producer_free(producer);

    return ATOMS.atomOk;
}

static void delivery_callback(pulsar_result result, pulsar_message_id_t* msg_id, void* ctx) {
    delivery_callback_ctx deliv_cb_ctx = *(delivery_callback_ctx *)ctx;
    ErlNifPid actual_pid = deliv_cb_ctx.callback_pid;

    ErlNifEnv* env = enif_alloc_env();

    char *p_message_id_str = pulsar_message_id_str(msg_id);
    ERL_NIF_TERM ret_bin = make_binary(env, p_message_id_str, strlen(p_message_id_str));

    const char* msg = deliv_cb_ctx.msg_data;
    ERL_NIF_TERM msg_bin = make_binary(env, msg, strlen(msg));

    if (result == pulsar_result_Ok) {
        enif_send(NULL, &actual_pid, env, enif_make_tuple4(env, ATOMS.atomDelivery, ATOMS.atomOk, ret_bin, msg_bin));
    } else {
        enif_send(NULL, &actual_pid, env, enif_make_tuple4(env, ATOMS.atomDelivery, ATOMS.atomError, ret_bin, msg_bin));
    }

    enif_release_resource(ctx);
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

    if (p_client->client == NULL)
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

    return enif_make_tuple2(env, ATOMS.atomOk, p_producer_res);
}

ERL_NIF_TERM async_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_producer *p_producer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void **) &p_producer))
    {
        return make_error_tuple(env, "couldn't retrieve producer resource from given reference");
    }

    if (p_producer->producer == NULL)
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
    maybe_set_message_options(env, message, argv[2]);

    delivery_callback_ctx *delivery_cb_ctx;
    delivery_cb_ctx = enif_alloc_resource(nif_delivery_callback_ctx_type, sizeof(delivery_callback_ctx));
    if (!delivery_cb_ctx)
    {
        return make_error_tuple(env, "no_memory for creating delivery callback context");
    }

    delivery_cb_ctx->callback_pid = p_producer->callback_pid;
    delivery_cb_ctx->msg_data = msg_str;

    pulsar_producer_send_async(p_producer->producer, message, delivery_callback, delivery_cb_ctx);

    pulsar_message_free(message);

    return ATOMS.atomOk;
}

ERL_NIF_TERM destroy_producer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    pulsar_producer *p_producer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void **) &p_producer))
    {
        return make_error_tuple(env, "couldn't retrieve producer resource from given reference");
    }

    if (p_producer->producer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed producer");
    }

    pulsar_producer_close(p_producer->producer);
    pulsar_producer_free(p_producer->producer);
    p_producer->producer = NULL;
    enif_release_resource(p_producer);

    return ATOMS.atomOk;
}

/*
 * Below is used for nif lifecycle
 */
static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{

    ATOMS.atomOk = make_atom(env, "ok");
    ATOMS.atomError = make_atom(env, "error");
    ATOMS.atomDelivery = make_atom(env, "delivery_callback");
    ATOMS.atomNeutronMsg = make_atom(env, "neutron_msg");

    ErlNifResourceType *rt_client;
    ErlNifResourceType *rt_consumer;
    ErlNifResourceType *rt_msg_id;
    ErlNifResourceType *rt_producer;
    ErlNifResourceType *rt_delivery_cb_ctx;

    rt_client = enif_open_resource_type(env, "neutron_nif", "pulsar_client", NULL, ERL_NIF_RT_CREATE, NULL);
    if (!rt_client) return -1;

    rt_consumer = enif_open_resource_type(env, "neutron_nif", "pulsar_consumer", NULL, ERL_NIF_RT_CREATE, NULL);
    if (!rt_consumer) return -1;

    rt_msg_id = enif_open_resource_type(env, "neutron_nif", "pulsar_msg_id", msg_id_destr, ERL_NIF_RT_CREATE, NULL);
    if (!rt_msg_id) return -1;

    rt_producer = enif_open_resource_type(env, "neutron_nif", "pulsar_producer", NULL, ERL_NIF_RT_CREATE, NULL);
    if (!rt_producer) return -1;

    rt_delivery_cb_ctx = enif_open_resource_type(env, "neutron_nif", "delivery_callback_ctx", NULL, ERL_NIF_RT_CREATE, NULL);
    if (!rt_delivery_cb_ctx) return -1;

    nif_pulsar_client_type = rt_client;
    nif_pulsar_consumer_type = rt_consumer;
    nif_pulsar_msg_id_type = rt_msg_id;
    nif_pulsar_producer_type = rt_producer;
    nif_delivery_callback_ctx_type = rt_delivery_cb_ctx;

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
    {"sync_produce", 4, sync_produce, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"make_client", 1, make_client, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"destroy_client", 1, destroy_client, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"do_consume", 2, do_consume, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ack", 2, ack, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ack_all", 2, ack_all, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nack", 2, nack},
    {"destroy_consumer", 1, destroy_consumer, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"create_async_producer", 3, create_async_producer, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"async_produce", 3, async_produce},
    {"destroy_producer", 1, destroy_producer, ERL_NIF_DIRTY_JOB_IO_BOUND},
};

ERL_NIF_INIT(Elixir.Neutron.PulsarNifs, nif_funcs, on_load, on_reload, on_upgrade, NULL)
