#include <erl_nif.h>

#include "pulsar/c/client.h"

#include <stdio.h>

#include <string.h>

typedef struct
{
    ERL_NIF_TERM atomAckGroupingMaxSize;
    ERL_NIF_TERM atomAckGroupingTimeMS;
    ERL_NIF_TERM atomBatchEnabled;
    ERL_NIF_TERM atomBatchMaxAllowedSizeInBytes;
    ERL_NIF_TERM atomBatchMaxMessages;
    ERL_NIF_TERM atomBatchMaxPublishDelayMS;
    ERL_NIF_TERM atomBlockIfQueueFull;
    ERL_NIF_TERM atomCallbackPID;
    ERL_NIF_TERM atomCompressionType;
    ERL_NIF_TERM atomConcurrentLookupRequest;
    ERL_NIF_TERM atomConsumerName;
    ERL_NIF_TERM atomConsumerType;
    ERL_NIF_TERM atomDeliverAfterMS;
    ERL_NIF_TERM atomDeliverAtMS;
    ERL_NIF_TERM atomDelivery;
    ERL_NIF_TERM atomError;
    ERL_NIF_TERM atomEventTimestamp;
    ERL_NIF_TERM atomHashingScheme;
    ERL_NIF_TERM atomIOThreads;
    ERL_NIF_TERM atomInitialSequenceID;
    ERL_NIF_TERM atomMaxPendingMessages;
    ERL_NIF_TERM atomMaxPendingMessagesAcrossPartitions;
    ERL_NIF_TERM atomMaxTotalReceiverQueueSizeAcrossPartitions;
    ERL_NIF_TERM atomMsgListenerThreads;
    ERL_NIF_TERM atomNegativeAckRedeliveryDelayMS;
    ERL_NIF_TERM atomNeutronMsg;
    ERL_NIF_TERM atomOk;
    ERL_NIF_TERM atomOperationTimeoutSeconds;
    ERL_NIF_TERM atomOrderingKey;
    ERL_NIF_TERM atomPartitionKey;
    ERL_NIF_TERM atomPartitionRoutingMode;
    ERL_NIF_TERM atomProducerName;
    ERL_NIF_TERM atomProperties;
    ERL_NIF_TERM atomReadCompacted;
    ERL_NIF_TERM atomReceiverQueueSize;
    ERL_NIF_TERM atomSendTimeoutMS;
    ERL_NIF_TERM atomSequenceID;
    ERL_NIF_TERM atomStatsIntervalInSeconds;
    ERL_NIF_TERM atomSubscriptionInitialPosition;
    ERL_NIF_TERM atomUnackedMessagesTimeoutMS;
} atoms;

atoms ATOMS;

typedef struct
{
    pulsar_client_t *client;
} pulsar_client;

typedef struct
{
    pulsar_consumer_t *consumer;
    ErlNifPid *callback_pid;
} pulsar_consumer;

typedef struct
{
    pulsar_message_id_t *msg_id;
} pulsar_msg_id;

typedef struct
{
    ERL_NIF_TERM topic;
    ErlNifPid *callback_pid;
    pulsar_producer_t *producer;
} pulsar_producer;

typedef struct
{
    ERL_NIF_TERM topic;
    ERL_NIF_TERM content;
    ERL_NIF_TERM options;
    ErlNifPid *callback_pid;
} delivery_callback_ctx;

static ErlNifResourceType *nif_pulsar_client_type = NULL;
static ErlNifResourceType *nif_pulsar_consumer_type = NULL;
static ErlNifResourceType *nif_pulsar_msg_id_type = NULL;
static ErlNifResourceType *nif_pulsar_producer_type = NULL;
static ErlNifResourceType *nif_delivery_callback_ctx_type = NULL;

static void client_destr(ErlNifEnv *env, void *obj)
{
    enif_fprintf(stdout, "*************************** auto destroying client \n");
    pulsar_client *p_client = (pulsar_client *)obj;

    if (p_client->client != NULL)
    {
        pulsar_client_close(p_client->client);
        pulsar_client_free(p_client->client);
        p_client->client = NULL;
    }
}

static void consumer_destr(ErlNifEnv *env, void *obj)
{
    enif_fprintf(stdout, "*************************** auto destroying consumer \n");
    pulsar_consumer *p_consumer = (pulsar_consumer *)obj;

    if (p_consumer->consumer != NULL)
    {
        pulsar_consumer_close(p_consumer->consumer);
        pulsar_consumer_free(p_consumer->consumer);
        p_consumer->consumer = NULL;
    }
}

static void msg_id_destr(ErlNifEnv *env, void *obj)
{
    enif_fprintf(stdout, "*************************** auto destroying msg id \n");
    pulsar_msg_id *p_msg_id = (pulsar_msg_id *)obj;

    if (p_msg_id->msg_id != NULL)
    {
        pulsar_message_id_free(p_msg_id->msg_id);
        p_msg_id->msg_id = NULL;
    }
}

static void producer_destr(ErlNifEnv *env, void *obj)
{
    enif_fprintf(stdout, "*************************** auto destroying producer\n");
    pulsar_producer *p_producer = (pulsar_producer *)obj;

    if (p_producer->producer != NULL)
    {
        pulsar_producer_flush(p_producer->producer);
        pulsar_producer_close(p_producer->producer);
        pulsar_producer_free(p_producer->producer);
        p_producer->producer = NULL;
    }
}

static ERL_NIF_TERM
make_atom(ErlNifEnv *env,
          const char *atom_name)
{
    ERL_NIF_TERM atom;

    if (enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1))
        return atom;

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env,
            const char *buff, size_t length)
{
    ERL_NIF_TERM term;
    uint8_t *destination_buffer = enif_make_new_binary(env, length, &term);
    memcpy(destination_buffer, buff, length);
    return term;
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv *env,
                 const char *error)
{
    return enif_make_tuple2(env, ATOMS.atomError,
                            make_binary(env, error, strlen(error)));
}

ERL_NIF_TERM create_client(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[])
{
    // service url
    ErlNifBinary url_bin;
    if (!enif_inspect_binary(env, argv[0], &url_bin))
    {
        return make_error_tuple(env,
                                "failed to create binary from input pulsar url");
    }
    const char *url = strndup((char *)url_bin.data, url_bin.size);
    enif_fprintf(stdout, "*************************** url_term %T \n", argv[0]);

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();

    // operation timeout
    ERL_NIF_TERM operation_timeout_seconds_term;
    int operation_timeout_seconds;
    if (enif_get_map_value(env, argv[1], ATOMS.atomOperationTimeoutSeconds, &operation_timeout_seconds_term) &&
        enif_get_int(env, operation_timeout_seconds_term, &operation_timeout_seconds))
    {
        enif_fprintf(stdout, "*************************** operation_timeout_seconds_term %T \n", operation_timeout_seconds_term);
        pulsar_client_configuration_set_operation_timeout_seconds(conf, operation_timeout_seconds);
    }

    // io threads
    ERL_NIF_TERM io_threads_term;
    int io_threads;
    if (enif_get_map_value(env, argv[1], ATOMS.atomIOThreads, &io_threads_term) && enif_get_int(env, io_threads_term, &io_threads))
    {
        enif_fprintf(stdout, "*************************** io_threads_term %T \n", io_threads_term);
        pulsar_client_configuration_set_io_threads(conf, io_threads);
    }

    // message listener threads
    ERL_NIF_TERM msg_listener_threads_term;
    int msg_listener_threads;
    if (enif_get_map_value(env, argv[1], ATOMS.atomMsgListenerThreads, &msg_listener_threads_term) && enif_get_int(env, msg_listener_threads_term, &msg_listener_threads))
    {
        enif_fprintf(stdout, "*************************** msg_listener_threads_term %T \n", msg_listener_threads_term);
        pulsar_client_configuration_set_message_listener_threads(conf, msg_listener_threads);
    }

    // concurrent lookup request
    ERL_NIF_TERM concurrent_lookup_request_term;
    int concurrent_lookup_request;
    if (enif_get_map_value(env, argv[1], ATOMS.atomConcurrentLookupRequest, &concurrent_lookup_request_term) &&
        enif_get_int(env, concurrent_lookup_request_term, &concurrent_lookup_request))
    {
        enif_fprintf(stdout, "*************************** concurrent_lookup_request_term %T \n", concurrent_lookup_request_term);
        pulsar_client_configuration_set_concurrent_lookup_request(conf, concurrent_lookup_request);
    }

    // stats interval
    ERL_NIF_TERM stats_interval_in_seconds_term;
    unsigned int stats_interval_in_seconds;
    if (enif_get_map_value(env, argv[1], ATOMS.atomStatsIntervalInSeconds, &stats_interval_in_seconds_term) &&
        enif_get_uint(env, stats_interval_in_seconds_term, &stats_interval_in_seconds))
    {
        enif_fprintf(stdout, "*************************** stats_interval_in_seconds_term %T \n", stats_interval_in_seconds_term);
        pulsar_client_configuration_set_stats_interval_in_seconds(conf, stats_interval_in_seconds);
    }

    pulsar_client_t *client = pulsar_client_create(url, conf);
    pulsar_client_configuration_free(conf);

    pulsar_client *p_client;
    p_client =
        enif_alloc_resource(nif_pulsar_client_type, sizeof(pulsar_client));
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

ERL_NIF_TERM destroy_client(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[])
{
    enif_fprintf(stdout, "*************************** force destroying client \n");
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **)&p_client))
    {
        return make_error_tuple(env,
                                "couldn't retrieve client resource from given reference");
    }

    if (p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    pulsar_client_close(p_client->client);
    pulsar_client_free(p_client->client);
    p_client->client = NULL;

    return ATOMS.atomOk;
}

static void
listener_callback(pulsar_consumer_t *consumer, pulsar_message_t *message, void *ctx)
{
    ErlNifPid actual_pid = *(ErlNifPid *)ctx;

    ErlNifEnv *env = enif_alloc_env();

    // topic
    const char *topic = pulsar_message_get_topic_name(message);
    ERL_NIF_TERM ret_topic = make_binary(env, topic, strlen(topic));

    // id
    pulsar_msg_id *p_msg_id;
    p_msg_id =
        enif_alloc_resource(nif_pulsar_msg_id_type, sizeof(pulsar_msg_id));

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
    ERL_NIF_TERM props_keys[props_size];
    ERL_NIF_TERM props_vals[props_size];
    for (int i = 0; i < props_size; i++)
    {
        const char *key = pulsar_string_map_get_key(props, i);
        props_keys[i] = make_binary(env, key, strlen(key));
        const char *val = pulsar_string_map_get_value(props, i);
        props_vals[i] = make_binary(env, val, strlen(val));
    }
    ERL_NIF_TERM ret_props;
    enif_make_map_from_arrays(env, props_keys, props_vals, props_size, &ret_props);
    pulsar_string_map_free(props);

    // payload
    ERL_NIF_TERM ret_payload =
        make_binary(env, pulsar_message_get_data(message),
                    pulsar_message_get_length(message));

    ERL_NIF_TERM ret[10] = { ATOMS.atomNeutronMsg, ret_topic,
                             p_msg_id_res, ret_part_key, ret_order_key,
                             ret_publish_ts, ret_event_ts, ret_redeliver_ct, ret_props,
                             ret_payload };

    enif_send(NULL, &actual_pid, env,
              enif_make_tuple_from_array(env, ret, 10));

    pulsar_message_free(message);
    enif_free_env(env);
}

ERL_NIF_TERM
create_consumer(ErlNifEnv *env, int argc,
                const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **)&p_client))
    {
        return make_error_tuple(env,
                                "couldn't retrieve client resource from given reference");
    }

    if (p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    // topic
    ErlNifBinary topic_bin;
    if (!enif_inspect_binary(env, argv[1], &topic_bin))
    {
        return make_error_tuple(env,
                                "failed to create binary from input pulsar topic");
    }
    const char *topic = strndup((char *)topic_bin.data, topic_bin.size);

    // subscription
    ErlNifBinary subscription_bin;
    if (!enif_inspect_binary(env, argv[2], &subscription_bin))
    {
        return make_error_tuple(env,
                                "failed to create binary from input pulsar subscription");
    }
    const char *subscription = strndup((char *)subscription_bin.data, subscription_bin.size);

    // callback pid
    ErlNifPid callback_pid;
    if (!enif_get_local_pid(env, argv[3], &callback_pid))
    {
        return make_error_tuple(env,
                                "failed to make pulsar consumer callback_pid");
    }

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_configuration_set_message_listener(consumer_conf, listener_callback, &callback_pid);

    // consumer type
    ERL_NIF_TERM consumer_type_term;
    int consumer_type;
    if (enif_get_map_value(env, argv[4], ATOMS.atomConsumerType, &consumer_type_term) && enif_get_int(env, consumer_type_term, &consumer_type))
    {
        enif_fprintf(stdout, "*************************** consumer_type_term %T \n", consumer_type_term);
        pulsar_consumer_configuration_set_consumer_type(consumer_conf, (pulsar_consumer_type)consumer_type);
    }

    // receiver queue size
    ERL_NIF_TERM receiver_queue_size_term;
    int receiver_queue_size;
    if (enif_get_map_value(env, argv[4], ATOMS.atomReceiverQueueSize, &receiver_queue_size_term) &&
        enif_get_int(env, receiver_queue_size_term, &receiver_queue_size))
    {
        enif_fprintf(stdout, "*************************** receiver_queue_size_term %T \n", receiver_queue_size_term);
        pulsar_consumer_configuration_set_receiver_queue_size(consumer_conf,
                                                              receiver_queue_size);
    }

    // max total receiver queue size across partitions
    ERL_NIF_TERM max_total_receiver_queue_size_across_partitions_term;
    int max_total_receiver_queue_size_across_partitions;
    if (enif_get_map_value(env, argv[4], ATOMS.atomMaxTotalReceiverQueueSizeAcrossPartitions, &max_total_receiver_queue_size_across_partitions_term) &&
        enif_get_int(env,
                     max_total_receiver_queue_size_across_partitions_term, &max_total_receiver_queue_size_across_partitions))
    {
        enif_fprintf(stdout, "*************************** max_total_receiver_queue_size_across_partitions_term %T \n", max_total_receiver_queue_size_across_partitions_term);
        pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(consumer_conf, max_total_receiver_queue_size_across_partitions);
    }

    // consumer name
    ERL_NIF_TERM consumer_name_term;
    ErlNifBinary consumer_name_bin;
    if (enif_get_map_value(env, argv[4], ATOMS.atomConsumerName, &consumer_name_term) &&
        enif_inspect_binary(env, consumer_name_term, &consumer_name_bin))
    {
        const char *consumer_name =
            strndup((char *)consumer_name_bin.data, consumer_name_bin.size);
        enif_fprintf(stdout, "*************************** consumer_name_term %T \n", consumer_name_term);
        pulsar_consumer_set_consumer_name(consumer_conf, consumer_name);
    }

    // unacked messages timeout
    ERL_NIF_TERM unacked_messages_timeout_ms_term;
    unsigned long unacked_messages_timeout_ms;
    if (enif_get_map_value(env, argv[4], ATOMS.atomUnackedMessagesTimeoutMS, &unacked_messages_timeout_ms_term) &&
        enif_get_ulong(env, unacked_messages_timeout_ms_term, &unacked_messages_timeout_ms))
    {
        enif_fprintf(stdout, "*************************** unacked_messages_timeout_ms_term %T \n", unacked_messages_timeout_ms_term);
        pulsar_consumer_set_unacked_messages_timeout_ms(consumer_conf,
                                         ( uint64_t)              unacked_messages_timeout_ms);
    }

    // negative ack redelivery delay ms
    ERL_NIF_TERM negative_ack_redelivery_delay_ms_term;
    long negative_ack_redelivery_delay_ms;
    if (enif_get_map_value(env, argv[4], ATOMS.atomNegativeAckRedeliveryDelayMS, &negative_ack_redelivery_delay_ms_term) &&
        enif_get_long(env, negative_ack_redelivery_delay_ms_term, &negative_ack_redelivery_delay_ms))
    {
        enif_fprintf(stdout, "*************************** negative_ack_redelivery_delay_ms_term %T \n", negative_ack_redelivery_delay_ms_term);
        pulsar_configure_set_negative_ack_redelivery_delay_ms(consumer_conf,
                                                              negative_ack_redelivery_delay_ms);
    }

    // ack grouping time ms
    ERL_NIF_TERM ack_grouping_time_ms_term;
    long ack_grouping_time_ms;
    if (enif_get_map_value(env, argv[4], ATOMS.atomAckGroupingTimeMS, &ack_grouping_time_ms_term) &&
        enif_get_long(env, ack_grouping_time_ms_term, &ack_grouping_time_ms))
    {
        enif_fprintf(stdout, "*************************** ack_grouping_time_ms_term %T \n", ack_grouping_time_ms_term);
        pulsar_configure_set_ack_grouping_time_ms(consumer_conf,
                                                  ack_grouping_time_ms);
    }

    // ack grouping max size
    ERL_NIF_TERM ack_grouping_max_size_term;
    long ack_grouping_max_size;
    if (enif_get_map_value(env, argv[4], ATOMS.atomAckGroupingMaxSize, &ack_grouping_max_size_term) &&
        enif_get_long(env, ack_grouping_max_size_term, &ack_grouping_max_size))
    {
        enif_fprintf(stdout, "*************************** ack_grouping_max_size_term %T \n", ack_grouping_max_size_term);
        pulsar_configure_set_ack_grouping_max_size(consumer_conf,
                                                   ack_grouping_max_size);
    }

    // read compact
    ERL_NIF_TERM read_compacted_term;
    int read_compacted;
    if (enif_get_map_value(env, argv[4], ATOMS.atomReadCompacted, &read_compacted_term) &&
        enif_get_int(env, read_compacted_term, &read_compacted))
    {
        enif_fprintf(stdout, "*************************** read_compacted_term %T \n", read_compacted_term);
        pulsar_consumer_set_read_compacted(consumer_conf, read_compacted);
    }

    // subscription initial position
    ERL_NIF_TERM subscription_initial_position_term;
    int subscription_initial_position;
    if (enif_get_map_value(env, argv[4], ATOMS.atomSubscriptionInitialPosition, &subscription_initial_position_term) &&
        enif_get_int(env, subscription_initial_position_term, &subscription_initial_position))
    {
        enif_fprintf(stdout, "*************************** subscription_initial_position_term %T \n", subscription_initial_position_term);
        pulsar_consumer_set_subscription_initial_position(consumer_conf,
                                                          (initial_position)subscription_initial_position);
    }

    // properties
    ERL_NIF_TERM properties_term;
    if (enif_get_map_value(env, argv[4], ATOMS.atomProperties, &properties_term) && enif_is_map(env, properties_term))
    {
        ERL_NIF_TERM key_term, val_term;
        ErlNifMapIterator iter;
        enif_map_iterator_create(env, properties_term, &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &key_term, &val_term))
        {
            ErlNifBinary key_bin;
            ErlNifBinary val_bin;
            if (enif_inspect_binary(env, key_term, &key_bin) &&
                enif_inspect_binary(env, val_term, &val_bin))
            {
                const char *key =
                    strndup((char *)key_bin.data, key_bin.size);
                const char *val =
                    strndup((char *)val_bin.data, val_bin.size);
                enif_fprintf(stdout, "*************************** properties %T:   %T \n", key, val);
                pulsar_consumer_configuration_set_property(consumer_conf, key, val);
            }
            enif_map_iterator_next(env, &iter);
        }

        enif_map_iterator_destroy(env, &iter);
    }

    pulsar_consumer *p_consumer;
    p_consumer =
        enif_alloc_resource(nif_pulsar_consumer_type, sizeof(pulsar_consumer));
    if (!p_consumer)
    {
        return make_error_tuple(env, "no_memory for creating pulsar consumer");
    }
    p_consumer->consumer = NULL;
    p_consumer->callback_pid = &callback_pid;

    pulsar_consumer_t *consumer;
    pulsar_result res =
        pulsar_client_subscribe(p_client->client, topic, subscription, consumer_conf, &consumer);
    pulsar_consumer_configuration_free(consumer_conf);

    if (res != pulsar_result_Ok)
    {
        return make_error_tuple(env, "failed to make pulsar consumer");
    }

    p_consumer->consumer = consumer;
    ERL_NIF_TERM p_consumer_res = enif_make_resource(env, p_consumer);
    enif_release_resource(p_consumer);

    return enif_make_tuple2(env, ATOMS.atomOk, p_consumer_res);
}

ERL_NIF_TERM
ack(ErlNifEnv *env, int argc,
    const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **)&p_consumer))
    {
        return make_error_tuple(env,
                                "couldn't retrieve consumer resource from given reference");
    }

    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **)&p_msg_id))
    {
        return make_error_tuple(env,
                                "couldn't retrieve msg_id resource from given reference");
    }

    if (p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    pulsar_result res =
        pulsar_consumer_acknowledge_id(p_consumer->consumer, p_msg_id->msg_id);

    if (res != pulsar_result_Ok)
    {
        return make_error_tuple(env, "failed to ack");
    }
    else
    {
        return ATOMS.atomOk;
    }
}

ERL_NIF_TERM nack(ErlNifEnv *env, int argc,
                  const ERL_NIF_TERM argv[])
{
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **)&p_consumer))
    {
        return make_error_tuple(env,
                                "couldn't retrieve consumer resource from given reference");
    }

    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_msg_id *p_msg_id;
    if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void **)&p_msg_id))
    {
        return make_error_tuple(env,
                                "couldn't retrieve msg_id resource from given reference");
    }

    if (p_msg_id->msg_id == NULL)
    {
        return make_error_tuple(env, "passed-in an invalid msg_id");
    }

    // this API doesn't nack on servers and only on client
    // this can lead to unbounded growth with nacked msg_ids
    // but there's no mechanism to prevent this
    pulsar_consumer_negative_acknowledge_id(p_consumer->consumer,
                                            p_msg_id->msg_id);

    return ATOMS.atomOk;
}

ERL_NIF_TERM destroy_consumer(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[])
{
    enif_fprintf(stdout, "*************************** force destroying consumer \n");
    pulsar_consumer *p_consumer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void **)&p_consumer))
    {
        return make_error_tuple(env,
                                "couldn't retrieve consumer resource from given reference");
    }

    if (p_consumer->consumer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed consumer");
    }

    pulsar_consumer_close(p_consumer->consumer);
    pulsar_consumer_free(p_consumer->consumer);
    p_consumer->consumer = NULL;

    return ATOMS.atomOk;
}

static void delivery_callback(pulsar_result result, pulsar_message_id_t *msg_id, void *ctx)
{
    delivery_callback_ctx deliv_cb_ctx = *(delivery_callback_ctx *)ctx;
    if (enif_is_pid_undefined(deliv_cb_ctx.callback_pid))
    {
        pulsar_message_id_free(msg_id);
        return;
    }

    ErlNifEnv *env = enif_alloc_env();
    if (result == pulsar_result_Ok)
    {
        char *p_message_id_str = pulsar_message_id_str(msg_id);
        ERL_NIF_TERM p_msg_id_res =
            make_binary(env, p_message_id_str, strlen(p_message_id_str));

        enif_send(NULL, deliv_cb_ctx.callback_pid, env,
                  enif_make_tuple6(env, ATOMS.atomDelivery, ATOMS.atomOk,
                                   p_msg_id_res, deliv_cb_ctx.topic, deliv_cb_ctx.content, deliv_cb_ctx.options));
    }
    else
    {
        enif_send(NULL, deliv_cb_ctx.callback_pid, env,
                  enif_make_tuple5(env, ATOMS.atomDelivery, ATOMS.atomError,
                                   deliv_cb_ctx.topic, deliv_cb_ctx.content, deliv_cb_ctx.options));
    }

    pulsar_message_id_free(msg_id);
    enif_free_env(env);
}

ERL_NIF_TERM
create_producer(ErlNifEnv *env, int argc,
                const ERL_NIF_TERM argv[])
{
    pulsar_client *p_client;
    if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void **)&p_client))
    {
        return make_error_tuple(env,
                                "couldn't retrieve client resource from given reference");
    }

    if (p_client->client == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed client");
    }

    ErlNifBinary topic_bin;
    if (!enif_inspect_binary(env, argv[1], &topic_bin))
    {
        return make_error_tuple(env,
                                "failed to create topic binary from input");
    }
    const char *topic = strndup((char *)topic_bin.data, topic_bin.size);

    pulsar_producer_configuration_t *producer_conf =
        pulsar_producer_configuration_create();

    // producer name
    ERL_NIF_TERM producer_name_term;
    ErlNifBinary producer_name_bin;
    if (enif_get_map_value(env, argv[2], ATOMS.atomProducerName, &producer_name_term) &&
        enif_inspect_binary(env, producer_name_term, &producer_name_bin))
    {
        const char *producer_name =
            strndup((char *)producer_name_bin.data, producer_name_bin.size);
        enif_fprintf(stdout, "*************************** producer_name_term %T \n", producer_name_term);
        pulsar_producer_configuration_set_producer_name(producer_conf, producer_name);
    }

    // send timeout ms
    ERL_NIF_TERM send_timeout_ms_term;
    int send_timeout_ms;
    if (enif_get_map_value(env, argv[2], ATOMS.atomSendTimeoutMS, &send_timeout_ms_term) &&
        enif_get_int(env, send_timeout_ms_term, &send_timeout_ms))
    {
        enif_fprintf(stdout, "*************************** send_timeout_ms_term %T \n", send_timeout_ms_term);
        pulsar_producer_configuration_set_send_timeout(producer_conf, send_timeout_ms);
    }

    // initial sequence id
    ERL_NIF_TERM initial_sequence_id_term;
    long initial_sequence_id;
    if (enif_get_map_value(env, argv[2], ATOMS.atomInitialSequenceID, &initial_sequence_id_term) &&
        enif_get_long(env, initial_sequence_id_term, &initial_sequence_id))
    {
        enif_fprintf(stdout, "*************************** initial_sequence_id_term %T \n", initial_sequence_id_term);
        pulsar_producer_configuration_set_initial_sequence_id(producer_conf,(int64_t) initial_sequence_id);
    }

    // compression type
    ERL_NIF_TERM compression_type_term;
    int compression_type;
    if (enif_get_map_value(env, argv[2], ATOMS.atomCompressionType, &compression_type_term) &&
        enif_get_int(env, compression_type_term, &compression_type))
    {
        enif_fprintf(stdout, "*************************** compression_type_term %T \n", compression_type_term);
        pulsar_producer_configuration_set_compression_type(producer_conf, (pulsar_compression_type)compression_type);
    }

    // max pending messages
    ERL_NIF_TERM max_pending_messages_term;
    int max_pending_messages;
    if (enif_get_map_value(env, argv[2], ATOMS.atomMaxPendingMessages, &max_pending_messages_term) &&
        enif_get_int(env, max_pending_messages_term, &max_pending_messages))
    {
        enif_fprintf(stdout, "*************************** max_pending_messages_term %T \n", max_pending_messages_term);
        pulsar_producer_configuration_set_max_pending_messages(producer_conf, max_pending_messages);
    }

    // max pending messages across partitions
    ERL_NIF_TERM max_pending_messages_across_partitions_term;
    int max_pending_messages_across_partitions;
    if (enif_get_map_value(env, argv[2], ATOMS.atomMaxPendingMessagesAcrossPartitions, &max_pending_messages_across_partitions_term) &&
        enif_get_int(env, max_pending_messages_across_partitions_term, &max_pending_messages_across_partitions))
    {
        enif_fprintf(stdout, "*************************** max_pending_messages_across_partitions_term %T \n", max_pending_messages_across_partitions_term);
        pulsar_producer_configuration_set_max_pending_messages_across_partitions(producer_conf, max_pending_messages_across_partitions);
    }

    // partition routing mode
    ERL_NIF_TERM partition_routing_mode_term;
    int partition_routing_mode;
    if (enif_get_map_value(env, argv[2], ATOMS.atomPartitionRoutingMode, &partition_routing_mode_term) &&
        enif_get_int(env, partition_routing_mode_term, &partition_routing_mode))
    {
        enif_fprintf(stdout, "*************************** partition_routing_mode_term %T \n", partition_routing_mode_term);

        pulsar_producer_configuration_set_partitions_routing_mode(producer_conf, (pulsar_partitions_routing_mode)partition_routing_mode);
    }

    // hashing scheme
    ERL_NIF_TERM hashing_scheme_term;
    int hashing_scheme;
    if (enif_get_map_value(env, argv[2], ATOMS.atomHashingScheme, &hashing_scheme_term) &&
        enif_get_int(env, hashing_scheme_term, &hashing_scheme))
    {
        enif_fprintf(stdout, "*************************** hashing_scheme_term %T \n", hashing_scheme_term);

        pulsar_producer_configuration_set_hashing_scheme(producer_conf, (pulsar_hashing_scheme)hashing_scheme);
    }

    // block if queue full
    ERL_NIF_TERM block_if_queue_full_term;
    int block_if_queue_full;
    if (enif_get_map_value(env, argv[2], ATOMS.atomBlockIfQueueFull, &block_if_queue_full_term) &&
        enif_get_int(env, block_if_queue_full_term, &block_if_queue_full))
    {
        enif_fprintf(stdout, "*************************** block_if_queue_full_term %T \n", block_if_queue_full_term);

        pulsar_producer_configuration_set_block_if_queue_full(producer_conf, block_if_queue_full);
    }

    // batch enqbled
    ERL_NIF_TERM batch_enabled_term;
    int batch_enabled;
    if (enif_get_map_value(env, argv[2], ATOMS.atomBatchEnabled, &batch_enabled_term) &&
        enif_get_int(env, batch_enabled_term, &batch_enabled))
    {
        enif_fprintf(stdout, "*************************** batch_enabled_term %T \n", batch_enabled_term);

        pulsar_producer_configuration_set_batching_enabled(producer_conf, batch_enabled);
    }

    // batching max messages
    ERL_NIF_TERM batch_max_messages_term;
    unsigned int batch_max_messages;
    if (enif_get_map_value(env, argv[2], ATOMS.atomBatchMaxMessages, &batch_max_messages_term) &&
        enif_get_uint(env, batch_max_messages_term, &batch_max_messages))
    {
        enif_fprintf(stdout, "*************************** batch_max_messages_term %T \n", batch_max_messages_term);

        pulsar_producer_configuration_set_batching_max_messages(producer_conf, batch_max_messages);
    }

    // batching max allowed size in bytes
    ERL_NIF_TERM batching_max_allowed_size_in_bytes_term;
    unsigned long batching_max_allowed_size_in_bytes;
    if (enif_get_map_value(env, argv[2], ATOMS.atomBatchMaxAllowedSizeInBytes, &batching_max_allowed_size_in_bytes_term) &&
        enif_get_ulong(env, batching_max_allowed_size_in_bytes_term, &batching_max_allowed_size_in_bytes))
    {
        enif_fprintf(stdout, "*************************** batching_max_allowed_size_in_bytes_term %T \n", batching_max_allowed_size_in_bytes_term);

        pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(producer_conf, batching_max_allowed_size_in_bytes);
    }

    // batching max allowed size in bytes
    ERL_NIF_TERM batching_max_publish_delay_ms_term;
    unsigned long batching_max_publish_delay_ms;
    if (enif_get_map_value(env, argv[2], ATOMS.atomBatchMaxPublishDelayMS, &batching_max_publish_delay_ms_term) &&
        enif_get_ulong(env, batching_max_publish_delay_ms_term, &batching_max_publish_delay_ms))
    {
        enif_fprintf(stdout, "*************************** batching_max_publish_delay_ms_term %T \n", batching_max_publish_delay_ms_term);

        pulsar_producer_configuration_set_batching_max_publish_delay_ms(producer_conf, batching_max_publish_delay_ms);
    }

    // properties
    ERL_NIF_TERM properties_term;
    if (enif_get_map_value(env, argv[2], ATOMS.atomProperties, &properties_term) && enif_is_map(env, properties_term))
    {
        ERL_NIF_TERM key_term, val_term;
        ErlNifMapIterator iter;
        enif_map_iterator_create(env, properties_term, &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &key_term, &val_term))
        {
            ErlNifBinary key_bin;
            ErlNifBinary val_bin;
            if (enif_inspect_binary(env, key_term, &key_bin) &&
                enif_inspect_binary(env, val_term, &val_bin))
            {
                const char *key =
                    strndup((char *)key_bin.data, key_bin.size);
                const char *val =
                    strndup((char *)val_bin.data, val_bin.size);
                enif_fprintf(stdout, "*************************** properties %T:  %T \n", key, val);

                pulsar_producer_configuration_set_property(producer_conf, key, val);
            }
            enif_map_iterator_next(env, &iter);
        }

        enif_map_iterator_destroy(env, &iter);
    }

    pulsar_producer_t *producer;

    pulsar_result res =
        pulsar_client_create_producer(p_client->client, topic, producer_conf, &producer);

    pulsar_producer_configuration_free(producer_conf);

    if (res != pulsar_result_Ok)
    {
        return make_error_tuple(env, "failed to make pulsar producer");
    }

    pulsar_producer *p_producer;

    p_producer =
        enif_alloc_resource(nif_pulsar_producer_type, sizeof(pulsar_producer));
    if (!p_producer)
    {
        return make_error_tuple(env, "no_memory for creating pulsar producer");
    }

    // producer callback pid
    p_producer->topic = argv[1];
    ErlNifPid p_callback_pid;
    enif_set_pid_undefined(&p_callback_pid);
    p_producer->callback_pid = &p_callback_pid;

    ERL_NIF_TERM callback_pid_term;
    ErlNifPid callback_pid;
    if (enif_get_map_value(env, argv[2], ATOMS.atomCallbackPID, &callback_pid_term) && enif_get_local_pid(env, callback_pid_term, &callback_pid))
    {
        p_producer->callback_pid = &callback_pid;
    }
    p_producer->producer = NULL;
    p_producer->producer = producer;

    ERL_NIF_TERM p_producer_res = enif_make_resource(env, p_producer);
    enif_release_resource(p_producer);

    return enif_make_tuple2(env, ATOMS.atomOk, p_producer_res);
}

ERL_NIF_TERM produce(ErlNifEnv *env, int argc,
                     const ERL_NIF_TERM argv[])
{
    pulsar_producer *p_producer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void **)&p_producer))
    {
        return make_error_tuple(env,
                                "couldn't retrieve producer resource from given reference");
    }

    if (p_producer->producer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed producer");
    }

    ErlNifBinary content_bin;
    if (!enif_inspect_binary(env, argv[1], &content_bin))
    {
        return make_error_tuple(env,
                                "failed to create message binary from input");
    }
    const char *content = strndup((char *)content_bin.data, content_bin.size);

    pulsar_message_t *message = pulsar_message_create();
    pulsar_message_set_content(message, content, strlen(content));

    // partition key
    ERL_NIF_TERM partition_key_term;
    ErlNifBinary partition_key_bin;
    if (enif_get_map_value(env, argv[2], ATOMS.atomPartitionKey, &partition_key_term) &&
        enif_inspect_binary(env, partition_key_term, &partition_key_bin))
    {
        const char *partition_key =
            strndup((char *)partition_key_bin.data, partition_key_bin.size);
        pulsar_message_set_partition_key(message, partition_key);
    }

    // ordering key
    ERL_NIF_TERM ordering_key_term;
    ErlNifBinary ordering_key_bin;
    if (enif_get_map_value(env, argv[2], ATOMS.atomOrderingKey, &ordering_key_term) &&
        enif_inspect_binary(env, ordering_key_term, &ordering_key_bin))
    {
        const char *ordering_key =
            strndup((char *)ordering_key_bin.data, ordering_key_bin.size);
        pulsar_message_set_ordering_key(message, ordering_key);
    }

    // event timestamp
    ERL_NIF_TERM event_timestamp_term;
    unsigned long event_timestamp;
    if (enif_get_map_value(env, argv[2], ATOMS.atomEventTimestamp, &event_timestamp_term) &&
        enif_get_ulong(env, event_timestamp_term, &event_timestamp))
    {
        pulsar_message_set_event_timestamp(message,(uint64_t) event_timestamp);
    }

    // sequence id
    ERL_NIF_TERM sequence_id_term;
    long sequence_id;
    if (enif_get_map_value(env, argv[2], ATOMS.atomSequenceID, &sequence_id_term) &&
        enif_get_long(env, sequence_id_term, &sequence_id))
    {
        pulsar_message_set_sequence_id(message,(int64_t) sequence_id);
    }

    // deliver after
    ERL_NIF_TERM deliver_after_ms_term;
    unsigned long deliver_after_ms;
    if (enif_get_map_value(env, argv[2], ATOMS.atomDeliverAfterMS, &deliver_after_ms_term) &&
        enif_get_ulong(env, deliver_after_ms_term, &deliver_after_ms))
    {
        pulsar_message_set_deliver_after(message,(uint64_t) deliver_after_ms);
    }

    // deliver at
    ERL_NIF_TERM deliver_at_ms_term;
    unsigned long deliver_at_ms;
    if (enif_get_map_value(env, argv[2], ATOMS.atomDeliverAtMS, &deliver_at_ms_term) &&
        enif_get_ulong(env, deliver_at_ms_term, &deliver_at_ms))
    {
        pulsar_message_set_deliver_at(message,(uint64_t) deliver_at_ms);
    }

    // properties
    ERL_NIF_TERM properties_term;
    if (enif_get_map_value(env, argv[2], ATOMS.atomProperties, &properties_term) &&
        enif_is_map(env, properties_term))
    {
        ERL_NIF_TERM key_term, val_term;
        ErlNifMapIterator iter;
        enif_map_iterator_create(env, properties_term, &iter,
                                 ERL_NIF_MAP_ITERATOR_FIRST);

        while (enif_map_iterator_get_pair(env, &iter, &key_term, &val_term))
        {
            ErlNifBinary key_bin;
            ErlNifBinary val_bin;
            if (enif_inspect_binary(env, key_term, &key_bin) &&
                enif_inspect_binary(env, val_term, &val_bin))
            {
                const char *key =
                    strndup((char *)key_bin.data, key_bin.size);
                const char *val =
                    strndup((char *)val_bin.data, val_bin.size);
                pulsar_message_set_property(message, key, val);
            }
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
    }

    delivery_callback_ctx *ctx;
    ctx =
        enif_alloc_resource(nif_delivery_callback_ctx_type,
                            sizeof(delivery_callback_ctx));
    if (!ctx)
    {
        return make_error_tuple(env,
                                "no_memory for creating delivery callback context");
    }

    ctx->callback_pid = p_producer->callback_pid;
    ctx->topic = p_producer->topic;
    ctx->content = argv[1];
    ctx->options = argv[2];

    // custom callback pid
    ERL_NIF_TERM callback_pid_term;
    ErlNifPid callback_pid;
    if (enif_get_map_value(env, argv[2], ATOMS.atomCallbackPID, &callback_pid_term) && enif_get_local_pid(env, callback_pid_term, &callback_pid))
    {
        ctx->callback_pid = &callback_pid;
    }

    pulsar_producer_send_async(p_producer->producer, message,
                               delivery_callback, ctx);

    enif_release_resource(ctx);
    pulsar_message_free(message);

    return ATOMS.atomOk;
}

ERL_NIF_TERM destroy_producer(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[])
{
    enif_fprintf(stdout, "*************************** force destroying producer \n");
    pulsar_producer *p_producer;
    if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void **)&p_producer))
    {
        return make_error_tuple(env,
                                "couldn't retrieve producer resource from given reference");
    }

    if (p_producer->producer == NULL)
    {
        return make_error_tuple(env, "passed-in a destroyed producer");
    }

    pulsar_producer_flush(p_producer->producer);
    pulsar_producer_close(p_producer->producer);
    pulsar_producer_free(p_producer->producer);
    p_producer->producer = NULL;

    return ATOMS.atomOk;
}

/*
 *Below is used for nif lifecycle
 */
static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    ATOMS.atomAckGroupingMaxSize = make_atom(env, "ack_grouping_max_size");
    ATOMS.atomAckGroupingTimeMS = make_atom(env, "ack_grouping_time_ms");
    ATOMS.atomBatchEnabled = make_atom(env, "batch_enabled");
    ATOMS.atomBatchMaxAllowedSizeInBytes = make_atom(env, "batch_max_allowed_size_in_bytes");
    ATOMS.atomBatchMaxMessages = make_atom(env, "batch_max_messages");
    ATOMS.atomBatchMaxPublishDelayMS = make_atom(env, "batch_max_publish_delay_ms");
    ATOMS.atomBlockIfQueueFull = make_atom(env, "block_if_queue_full");
    ATOMS.atomCallbackPID = make_atom(env, "callback_pid");
    ATOMS.atomCompressionType = make_atom(env, "compression_type");
    ATOMS.atomConcurrentLookupRequest = make_atom(env, "concurrent_lookup_request");
    ATOMS.atomConsumerName = make_atom(env, "consumer_name");
    ATOMS.atomConsumerType = make_atom(env, "consumer_type");
    ATOMS.atomDeliverAfterMS = make_atom(env, "deliver_after_ms");
    ATOMS.atomDeliverAtMS = make_atom(env, "deliver_at_ms");
    ATOMS.atomDelivery = make_atom(env, "delivery");
    ATOMS.atomError = make_atom(env, "error");
    ATOMS.atomEventTimestamp = make_atom(env, "event_timestamp");
    ATOMS.atomHashingScheme = make_atom(env, "hashing_scheme");
    ATOMS.atomIOThreads = make_atom(env, "io_threads");
    ATOMS.atomInitialSequenceID = make_atom(env, "initial_sequence_id");
    ATOMS.atomMaxPendingMessages = make_atom(env, "max_pending_messages");
    ATOMS.atomMaxPendingMessagesAcrossPartitions = make_atom(env, "max_pending_messages_across_partitions");
    ATOMS.atomMaxTotalReceiverQueueSizeAcrossPartitions = make_atom(env, "max_total_receiver_queue_size_across_partitions");
    ATOMS.atomMsgListenerThreads = make_atom(env, "msg_listener_threads");
    ATOMS.atomNegativeAckRedeliveryDelayMS = make_atom(env, "negative_ack_redelivery_delay_ms");
    ATOMS.atomNeutronMsg = make_atom(env, "neutron_msg");
    ATOMS.atomOk = make_atom(env, "ok");
    ATOMS.atomOperationTimeoutSeconds = make_atom(env, "operation_timeout_seconds");
    ATOMS.atomOrderingKey = make_atom(env, "ordering_key");
    ATOMS.atomPartitionKey = make_atom(env, "partition_key");
    ATOMS.atomPartitionRoutingMode = make_atom(env, "partition_routing_mode");
    ATOMS.atomProducerName = make_atom(env, "producer_name");
    ATOMS.atomProperties = make_atom(env, "properties");
    ATOMS.atomReadCompacted = make_atom(env, "read_compacted");
    ATOMS.atomReceiverQueueSize = make_atom(env, "receiver_queue_size");
    ATOMS.atomSendTimeoutMS = make_atom(env, "send_timeout_ms");
    ATOMS.atomSequenceID = make_atom(env, "sequence_id");
    ATOMS.atomStatsIntervalInSeconds = make_atom(env, "stats_interval_in_seconds");
    ATOMS.atomSubscriptionInitialPosition = make_atom(env, "subscription_initial_position");
    ATOMS.atomUnackedMessagesTimeoutMS = make_atom(env, "unacked_messages_timeout_ms");

    ErlNifResourceType *rt_client;
    ErlNifResourceType *rt_consumer;
    ErlNifResourceType *rt_msg_id;
    ErlNifResourceType *rt_producer;
    ErlNifResourceType *rt_delivery_cb_ctx;

    rt_client =
        enif_open_resource_type(env, "neutron_nif", "pulsar_client", client_destr,
                                ERL_NIF_RT_CREATE, NULL);
    if (!rt_client)
        return -1;

    rt_consumer =
        enif_open_resource_type(env, "neutron_nif", "pulsar_consumer", consumer_destr,
                                ERL_NIF_RT_CREATE, NULL);
    if (!rt_consumer)
        return -1;

    rt_msg_id =
        enif_open_resource_type(env, "neutron_nif", "pulsar_msg_id", msg_id_destr,
                                ERL_NIF_RT_CREATE, NULL);
    if (!rt_msg_id)
        return -1;

    rt_producer =
        enif_open_resource_type(env, "neutron_nif", "pulsar_producer", producer_destr,
                                ERL_NIF_RT_CREATE, NULL);
    if (!rt_producer)
        return -1;

    rt_delivery_cb_ctx =
        enif_open_resource_type(env, "neutron_nif", "delivery_callback_ctx",
                                NULL, ERL_NIF_RT_CREATE, NULL);
    if (!rt_delivery_cb_ctx)
        return -1;

    nif_pulsar_client_type = rt_client;
    nif_pulsar_consumer_type = rt_consumer;
    nif_pulsar_msg_id_type = rt_msg_id;
    nif_pulsar_producer_type = rt_producer;
    nif_delivery_callback_ctx_type = rt_delivery_cb_ctx;

    return 0;
}

static int
on_reload(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static int
on_upgrade(ErlNifEnv *env, void **priv, void **old_priv_data,
           ERL_NIF_TERM load_info)
{
    return 0;
}

ErlNifFunc nif_funcs[] = {
    {"create_client",
     2,
     create_client,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"destroy_client",
     1,
     destroy_client,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"create_consumer",
     5,
     create_consumer,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ack",
     2,
     ack,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"nack",
     2,
     nack},
    {"destroy_consumer",
     1,
     destroy_consumer,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"create_producer",
     3,
     create_producer,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"produce",
     3,
     produce},
    {"destroy_producer",
     1,
     destroy_producer,
     ERL_NIF_DIRTY_JOB_IO_BOUND},
};

ERL_NIF_INIT(Elixir.Neutron.PulsarNifs, nif_funcs, on_load, on_reload, on_upgrade, NULL)

