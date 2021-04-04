#include <erl_nif.h>

#include "pulsar/c/client.h"

#include <stdio.h>

#include <string.h>

typedef struct {
  ERL_NIF_TERM atomOk;
  ERL_NIF_TERM atomError;
  ERL_NIF_TERM atomDelivery;
  ERL_NIF_TERM atomNeutronMsg;
  ERL_NIF_TERM atomUrl;
  ERL_NIF_TERM atomIOThreads;
  ERL_NIF_TERM atomMsgListenerThreads;
  ERL_NIF_TERM atomSendBackToPid;
  ERL_NIF_TERM atomSubscription;
  ERL_NIF_TERM atomTopic;
  ERL_NIF_TERM atomTypeInt;
  ERL_NIF_TERM atomDeliverAfterMS;
  ERL_NIF_TERM atomDeliverAtMS;
  ERL_NIF_TERM atomEventTS;
  ERL_NIF_TERM atomPublishTS;
  ERL_NIF_TERM atomPartitionKey;
  ERL_NIF_TERM atomProperties;
  ERL_NIF_TERM atomReceiverQueueSize;
  ERL_NIF_TERM atomMaxTotalReceiverQueueSizeAcrossPartitions;
  ERL_NIF_TERM atomConsumerName;
  ERL_NIF_TERM atomUnackedMessagesTimeoutMS;
  ERL_NIF_TERM atomNegativeAckRedeliveryDelayMS;
  ERL_NIF_TERM atomAckGroupingTimeMS;
  ERL_NIF_TERM atomAckGroupingMaxSize;
  ERL_NIF_TERM atomReadCompacted;
  ERL_NIF_TERM atomSubscriptionInitialPosition;
	ERL_NIF_TERM atomOperationTimeoutSeconds;
	ERL_NIF_TERM atomConcurrentLookupRequest;
	ERL_NIF_TERM atomStatsIntervalInSeconds;
}
atoms;

atoms ATOMS;

typedef struct {
  pulsar_client_t * client;
}
pulsar_client;

typedef struct {
  pulsar_consumer_t * consumer;
  ErlNifPid callback_pid;
}
pulsar_consumer;

typedef struct {
  pulsar_message_id_t * msg_id;
}
pulsar_msg_id;

typedef struct {
  pulsar_producer_t * producer;
  ErlNifPid callback_pid;
}
pulsar_producer;

typedef struct {
  ERL_NIF_TERM payload;
  ERL_NIF_TERM options;
  ErlNifPid callback_pid;
}
delivery_callback_ctx;

static ErlNifResourceType * nif_pulsar_client_type = NULL;
static ErlNifResourceType * nif_pulsar_consumer_type = NULL;
static ErlNifResourceType * nif_pulsar_msg_id_type = NULL;
static ErlNifResourceType * nif_pulsar_producer_type = NULL;
static ErlNifResourceType * nif_delivery_callback_ctx_type = NULL;

static void client_destr(ErlNifEnv * env, void * obj) {
  enif_fprintf(stdout, "*************************** auto destroying client \n");
  pulsar_client * p_client = (pulsar_client * ) obj;

  if (p_client -> client!= NULL){
		pulsar_client_close(p_client -> client);
		pulsar_client_free(p_client -> client);
		p_client -> client = NULL;
  }
}

static void consumer_destr(ErlNifEnv * env, void * obj) {
  enif_fprintf(stdout, "*************************** auto destroying consumer \n");
  pulsar_consumer * p_consumer = (pulsar_consumer * ) obj;

  if (p_consumer -> consumer!= NULL){
		pulsar_consumer_close(p_consumer -> consumer);
		pulsar_consumer_free(p_consumer -> consumer);
		p_consumer -> consumer = NULL;
  }
}

static void msg_id_destr(ErlNifEnv * env, void * obj) {
  enif_fprintf(stdout, "*************************** auto destroying msg id \n");
  pulsar_msg_id * p_msg_id = (pulsar_msg_id * ) obj;

  if (p_msg_id -> msg_id!= NULL){
		pulsar_message_id_free(p_msg_id -> msg_id);
		p_msg_id -> msg_id = NULL;
  }
}

static void producer_destr(ErlNifEnv * env, void * obj) {
  enif_fprintf(stdout, "*************************** auto destroying producer\n");
  pulsar_producer * p_producer = (pulsar_producer * ) obj;

  if (p_producer -> producer!= NULL){
		pulsar_producer_flush(p_producer->producer);
		pulsar_producer_close(p_producer -> producer);
		pulsar_producer_free(p_producer -> producer);
		p_producer -> producer = NULL;
  }
}

static ERL_NIF_TERM
make_atom(ErlNifEnv * env,
  const char * atom_name) {
  ERL_NIF_TERM atom;

  if (enif_make_existing_atom(env, atom_name, & atom, ERL_NIF_LATIN1))
    return atom;

  return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_binary(ErlNifEnv * env,
  const char * buff, size_t length) {
  ERL_NIF_TERM term;
  uint8_t * destination_buffer = enif_make_new_binary(env, length, & term);
  memcpy(destination_buffer, buff, length);
  return term;
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv * env,
  const char * error) {
  return enif_make_tuple2(env, ATOMS.atomError,
    make_binary(env, error, strlen(error)));
}

ERL_NIF_TERM make_client(ErlNifEnv * env, int argc,					const ERL_NIF_TERM argv[]) {
					ERL_NIF_TERM url, io_threads, msg_listener_threads;
					if (!enif_get_map_value(env, argv[0], ATOMS.atomUrl, & url)) {
						return make_error_tuple(env,
							"failed to make pulsar client url configuration");
					}

					if (!enif_get_map_value(env, argv[0], ATOMS.atomIOThreads, & io_threads)) {
						return make_error_tuple(env,
							"failed to make pulsar client io_threads configuration");
					}

					if (!enif_get_map_value(env, argv[0], ATOMS.atomMsgListenerThreads, & msg_listener_threads)) {
						return make_error_tuple(env,
							"failed to make pulsar client msg_listener_threads configuration");
					}

					ErlNifBinary bin;

					int ret = enif_inspect_binary(env, url, & bin);
					if (!ret) {
						return make_error_tuple(env,
							"failed to create binary from input pulsar url");
					}

					const char * pulsar_str = strndup((char * ) bin.data, bin.size);

					int io_threads_int;
					if (!enif_get_int(env, io_threads, & io_threads_int)) {
						return make_error_tuple(env,
							"failed to make pulsar client io_threads");
					}

					int msg_listener_threads_int;
					if (!enif_get_int(env, msg_listener_threads, & msg_listener_threads_int)) {
						return make_error_tuple(env,
							"failed to make pulsar client msg_listener_threads");
					}

					pulsar_client_configuration_t * conf = pulsar_client_configuration_create();
					pulsar_client_configuration_set_io_threads(conf, io_threads_int);
					pulsar_client_configuration_set_message_listener_threads(conf,
						msg_listener_threads_int);

					ERL_NIF_TERM operation_timeout_seconds_term;
					int operation_timeout_seconds;
					if (enif_get_map_value(env, argv[0], ATOMS.atomOperationTimeoutSeconds, & operation_timeout_seconds_term) &&
						 enif_get_int(env, operation_timeout_seconds_term, & operation_timeout_seconds) ) {
						pulsar_client_configuration_set_operation_timeout_seconds(conf, operation_timeout_seconds);
					}

					ERL_NIF_TERM concurrent_lookup_request_term;
					int concurrent_lookup_request;
					if (enif_get_map_value(env, argv[0], ATOMS.atomConcurrentLookupRequest, & concurrent_lookup_request_term) &&
						 enif_get_int(env, concurrent_lookup_request_term, & concurrent_lookup_request) ) {
						pulsar_client_configuration_set_concurrent_lookup_request(conf, concurrent_lookup_request);
					}

					ERL_NIF_TERM stats_interval_in_seconds_term;
					int stats_interval_in_seconds;
					if (enif_get_map_value(env, argv[0], ATOMS.atomStatsIntervalInSeconds, & stats_interval_in_seconds_term) &&
						 enif_get_int(env, stats_interval_in_seconds_term, & stats_interval_in_seconds) ) {
						pulsar_client_configuration_set_stats_interval_in_seconds(conf, stats_interval_in_seconds);
					}

					pulsar_client_t * client = pulsar_client_create(pulsar_str, conf);

					pulsar_client_configuration_free(conf);

					pulsar_client * p_client;

					p_client =
						enif_alloc_resource(nif_pulsar_client_type, sizeof(pulsar_client));
					if (!p_client) {
						return make_error_tuple(env, "no_memory for creating pulsar client");
					}

					p_client -> client = NULL;
					p_client -> client = client;

					ERL_NIF_TERM p_client_res = enif_make_resource(env, p_client);
					enif_release_resource(p_client);

					return enif_make_tuple2(env, ATOMS.atomOk, p_client_res);
}

ERL_NIF_TERM destroy_client(ErlNifEnv * env, int argc,  const ERL_NIF_TERM argv[]) {
  enif_fprintf(stdout, "*************************** force destroying client \n");
  pulsar_client * p_client;
  if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void ** ) & p_client)) {
    return make_error_tuple(env,
      "couldn't retrieve client resource from given reference");
  }

  if (p_client -> client == NULL) {
    return make_error_tuple(env, "passed-in a destroyed client");
  }

  pulsar_client_close(p_client -> client);
  pulsar_client_free(p_client -> client);
  p_client -> client = NULL;

  return ATOMS.atomOk;
}

static void
listener_callback(pulsar_consumer_t * consumer, pulsar_message_t * message, void * ctx) {
  ErlNifPid actual_pid = * (ErlNifPid * ) ctx;

  ErlNifEnv * env = enif_alloc_env();

  // topic
  const char * topic = pulsar_message_get_topic_name(message);
  ERL_NIF_TERM ret_topic = make_binary(env, topic, strlen(topic));

  // id
  pulsar_msg_id * p_msg_id;
  p_msg_id =
    enif_alloc_resource(nif_pulsar_msg_id_type, sizeof(pulsar_msg_id));

  p_msg_id -> msg_id = NULL;
  p_msg_id -> msg_id = pulsar_message_get_message_id(message);

  ERL_NIF_TERM p_msg_id_res = enif_make_resource(env, p_msg_id);
	enif_release_resource(p_msg_id);

  // partition key
  const char * part_key = pulsar_message_get_partitionKey(message);
  ERL_NIF_TERM ret_part_key = make_binary(env, part_key, strlen(part_key));

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
  pulsar_string_map_t * props = pulsar_message_get_properties(message);
  int props_size = pulsar_string_map_size(props);
  ERL_NIF_TERM props_keys[props_size];
  ERL_NIF_TERM props_vals[props_size];
  for (int i = 0; i < props_size; i++) {
    const char * key = pulsar_string_map_get_key(props, i);
    props_keys[i] = make_binary(env, key, strlen(key));
    const char * val = pulsar_string_map_get_value(props, i);
    props_vals[i] = make_binary(env, val, strlen(val));
  }
  ERL_NIF_TERM ret_props;
  enif_make_map_from_arrays(env, props_keys, props_vals, props_size, &
    ret_props);
  pulsar_string_map_free(props);

  // payload
  ERL_NIF_TERM ret_payload =
    make_binary(env, pulsar_message_get_data(message),
      pulsar_message_get_length(message));

  enif_send(NULL, & actual_pid, env,
    enif_make_tuple9(env, ATOMS.atomNeutronMsg, ret_topic,
      p_msg_id_res, ret_part_key, ret_publish_ts,
      ret_event_ts, ret_redeliver_ct, ret_props,
      ret_payload));

	pulsar_message_free(message);
	enif_free_env(env);
}

ERL_NIF_TERM
do_consume(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[]) {
					pulsar_client * p_client;
					if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void ** ) & p_client)) {
						return make_error_tuple(env,
							"couldn't retrieve client resource from given reference");
					}

					if (p_client -> client == NULL) {
						return make_error_tuple(env, "passed-in a destroyed client");
					}

					ERL_NIF_TERM send_back_to_pid_term, topic_term, subscription_term,
					type_int_term;
					if (!enif_get_map_value(env, argv[1], ATOMS.atomSendBackToPid, & send_back_to_pid_term)) {
						return make_error_tuple(env,
							"failed to make pulsar consumer send_back_to_pid configuration");
					}

					if (!enif_get_map_value(env, argv[1], ATOMS.atomSubscription, & subscription_term)) {
						return make_error_tuple(env,
							"failed to make pulsar consumer subscription configuration");
					}

					if (!enif_get_map_value(env, argv[1], ATOMS.atomTopic, & topic_term)) {
						return make_error_tuple(env,
							"failed to make pulsar consumer topic configuration");
					}

					if (!enif_get_map_value(env, argv[1], ATOMS.atomTypeInt, & type_int_term)) {
						return make_error_tuple(env,
							"failed to make pulsar consumer type configuration");
					}

					ErlNifPid send_back_to_pid;
					if (!enif_get_local_pid(env, send_back_to_pid_term, & send_back_to_pid)) {
						return make_error_tuple(env,
							"failed to make pulsar consumer send_back_to_pid");
					}

					ErlNifBinary sub_bin;
					int ret_sub = enif_inspect_binary(env, subscription_term, & sub_bin);
					if (!ret_sub) {
						return make_error_tuple(env,
							"failed to create binary from input pulsar subscription");
					}

					const char * subscription_str =
						strndup((char * ) sub_bin.data, sub_bin.size);

					ErlNifBinary bin_topic;
					int ret_topic = enif_inspect_binary(env, topic_term, & bin_topic);
					if (!ret_topic) {
						return make_error_tuple(env,
							"failed to create binary from input pulsar topic");
					}

					const char * topic_str = strndup((char * ) bin_topic.data, bin_topic.size);

					int consumer_type_int;
					if (!enif_get_int(env, type_int_term, & consumer_type_int)) {
						return make_error_tuple(env,
							"failed to create consumer type from input type int");
					}
					pulsar_consumer_type consumer_type =
						(pulsar_consumer_type) consumer_type_int;

					pulsar_consumer * p_consumer;

					p_consumer =
						enif_alloc_resource(nif_pulsar_consumer_type, sizeof(pulsar_consumer));
					if (!p_consumer) {
						return make_error_tuple(env, "no_memory for creating pulsar consumer");
					}

					p_consumer -> consumer = NULL;
					p_consumer -> callback_pid = send_back_to_pid;

					pulsar_consumer_configuration_t * consumer_conf =
						pulsar_consumer_configuration_create();
					pulsar_consumer_configuration_set_consumer_type(consumer_conf,
						consumer_type);
					pulsar_consumer_configuration_set_message_listener(consumer_conf,
						listener_callback, &
						p_consumer ->
						callback_pid);

					int receiver_queue_size;
					ERL_NIF_TERM receiver_queue_size_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomReceiverQueueSize, & receiver_queue_size_term) &&
						enif_get_int(env, receiver_queue_size_term, & receiver_queue_size)) {
						pulsar_consumer_configuration_set_receiver_queue_size(consumer_conf,
							receiver_queue_size);
					}

					int max_total_receiver_queue_size_across_partitions;
					ERL_NIF_TERM max_total_receiver_queue_size_across_partitions_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomMaxTotalReceiverQueueSizeAcrossPartitions, &
							max_total_receiver_queue_size_across_partitions_term) &&
						enif_get_int(env,
							max_total_receiver_queue_size_across_partitions_term, &
							max_total_receiver_queue_size_across_partitions)) {
						pulsar_consumer_set_max_total_receiver_queue_size_across_partitions
							(consumer_conf, max_total_receiver_queue_size_across_partitions);
					}

					unsigned long unacked_messages_timeout_ms;
					ERL_NIF_TERM unacked_messages_timeout_ms_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomUnackedMessagesTimeoutMS, &
							unacked_messages_timeout_ms_term) &&
						enif_get_uint64(env, unacked_messages_timeout_ms_term, &
							unacked_messages_timeout_ms)) {
						pulsar_consumer_set_unacked_messages_timeout_ms(consumer_conf,
							(uint64_t) unacked_messages_timeout_ms);
					}

					long negative_ack_redelivery_delay_ms;
					ERL_NIF_TERM negative_ack_redelivery_delay_ms_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomNegativeAckRedeliveryDelayMS, &
							negative_ack_redelivery_delay_ms_term) &&
						enif_get_int64(env, negative_ack_redelivery_delay_ms_term, &
							negative_ack_redelivery_delay_ms)) {
						pulsar_configure_set_negative_ack_redelivery_delay_ms(consumer_conf,
							negative_ack_redelivery_delay_ms);
					}

					long ack_grouping_time_ms;
					ERL_NIF_TERM ack_grouping_time_ms_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomAckGroupingTimeMS, & ack_grouping_time_ms_term) &&
						enif_get_int64(env, ack_grouping_time_ms_term, &
							ack_grouping_time_ms)) {
						pulsar_configure_set_ack_grouping_time_ms(consumer_conf,
							ack_grouping_time_ms);
					}

					long ack_grouping_max_size;
					ERL_NIF_TERM ack_grouping_max_size_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomAckGroupingMaxSize, &
							ack_grouping_max_size_term) &&
						enif_get_int64(env, ack_grouping_max_size_term, &
							ack_grouping_max_size)) {
						pulsar_configure_set_ack_grouping_max_size(consumer_conf,
							ack_grouping_max_size);
					}

					ErlNifBinary consumer_name_bin;
					ERL_NIF_TERM consumer_name_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomConsumerName, & consumer_name_term) &&
						enif_inspect_binary(env, consumer_name_term, & consumer_name_bin)) {
						const char * consumer_name_str =
							strndup((char * ) consumer_name_bin.data, consumer_name_bin.size);
						pulsar_consumer_set_consumer_name(consumer_conf, consumer_name_str);
					}

					int subscription_initial_position;
					ERL_NIF_TERM subscription_initial_position_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomSubscriptionInitialPosition, &
							subscription_initial_position_term) &&
						enif_get_int(env, subscription_initial_position_term, &
							subscription_initial_position)) {
						pulsar_consumer_set_subscription_initial_position(consumer_conf,
							(initial_position) subscription_initial_position);
					}

					int read_compacted;
					ERL_NIF_TERM read_compacted_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomReadCompacted, & read_compacted_term) &&
						enif_get_int(env, read_compacted_term, & read_compacted)) {
						pulsar_consumer_set_read_compacted(consumer_conf, read_compacted);
					}

					ERL_NIF_TERM properties_term;
					if (enif_get_map_value(env, argv[1], ATOMS.atomProperties, & properties_term) &&
						enif_is_map(env, properties_term)) {
						ERL_NIF_TERM key_term, val_term;
						ErlNifMapIterator iter;
						enif_map_iterator_create(env, properties_term, & iter,
							ERL_NIF_MAP_ITERATOR_FIRST);

						while (enif_map_iterator_get_pair(env, & iter, & key_term, & val_term)) {
							ErlNifBinary key_bin;
							ErlNifBinary val_bin;
							if (enif_inspect_binary(env, key_term, & key_bin) &&
								enif_inspect_binary(env, val_term, & val_bin)) {
								const char * key_str =
									strndup((char * ) key_bin.data, key_bin.size);
								const char * val_str =
									strndup((char * ) val_bin.data, val_bin.size);
								pulsar_consumer_configuration_set_property(consumer_conf,
									key_str, val_str);
							}
							enif_map_iterator_next(env, & iter);
						}

						enif_map_iterator_destroy(env, & iter);
					}

					pulsar_consumer_t * consumer;
					pulsar_result res =
						pulsar_client_subscribe(p_client -> client, topic_str, subscription_str,
							consumer_conf, & consumer);
          pulsar_consumer_configuration_free(consumer_conf);

					if (res != pulsar_result_Ok) {
						return make_error_tuple(env, "failed to make pulsar consumer");
					}

					p_consumer -> consumer = consumer;
					ERL_NIF_TERM p_consumer_res = enif_make_resource(env, p_consumer);
					enif_release_resource(p_consumer);

					return enif_make_tuple2(env, ATOMS.atomOk, p_consumer_res);
}


ERL_NIF_TERM
ack(ErlNifEnv * env, int argc,  const ERL_NIF_TERM argv[]) {
  pulsar_consumer * p_consumer;
  if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void ** ) & p_consumer)) {
    return make_error_tuple(env,
      "couldn't retrieve consumer resource from given reference");
  }

  if (p_consumer -> consumer == NULL) {
    return make_error_tuple(env, "passed-in a destroyed consumer");
  }

  pulsar_msg_id * p_msg_id;
  if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void ** ) & p_msg_id)) {
    return make_error_tuple(env,
      "couldn't retrieve msg_id resource from given reference");
  }

  if (p_msg_id -> msg_id == NULL) {
    return make_error_tuple(env, "passed-in an invalid msg_id");
  }

  pulsar_result res =
    pulsar_consumer_acknowledge_id(p_consumer -> consumer, p_msg_id -> msg_id);

  if (res != pulsar_result_Ok) {
    return make_error_tuple(env, "failed to ack");
  } else {
    return ATOMS.atomOk;
  }
}

ERL_NIF_TERM
ack_all(ErlNifEnv * env, int argc,  const ERL_NIF_TERM argv[]) {
  pulsar_consumer * p_consumer;
  if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void ** ) & p_consumer)) {
    return make_error_tuple(env,
      "couldn't retrieve consumer resource from given reference");
  }

  if (p_consumer -> consumer == NULL) {
    return make_error_tuple(env, "passed-in a destroyed consumer");
  }

  pulsar_msg_id * p_msg_id;
  if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void ** ) & p_msg_id)) {
    return make_error_tuple(env,
      "couldn't retrieve msg_id resource from given reference");
  }

  if (p_msg_id -> msg_id == NULL) {
    return make_error_tuple(env, "passed-in an invalid msg_id");
  }

  pulsar_result res =
    pulsar_consumer_acknowledge_cumulative_id(p_consumer -> consumer,
      p_msg_id -> msg_id);

  if (res != pulsar_result_Ok) {
    return make_error_tuple(env, "failed to ack_all");
  } else {
    return ATOMS.atomOk;
  }
}

ERL_NIF_TERM nack(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[]) {
  pulsar_consumer * p_consumer;
  if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void ** ) & p_consumer)) {
    return make_error_tuple(env,
      "couldn't retrieve consumer resource from given reference");
  }

  if (p_consumer -> consumer == NULL) {
    return make_error_tuple(env, "passed-in a destroyed consumer");
  }

  pulsar_msg_id * p_msg_id;
  if (!enif_get_resource(env, argv[1], nif_pulsar_msg_id_type, (void ** ) & p_msg_id)) {
    return make_error_tuple(env,
      "couldn't retrieve msg_id resource from given reference");
  }

  if (p_msg_id -> msg_id == NULL) {
    return make_error_tuple(env, "passed-in an invalid msg_id");
  }

  // this API doesn't nack on servers and only on client
  // this can lead to unbounded growth with nacked msg_ids
  // but there's no mechanism to prevent this
  pulsar_consumer_negative_acknowledge_id(p_consumer -> consumer,
    p_msg_id -> msg_id);

  return ATOMS.atomOk;
}

ERL_NIF_TERM destroy_consumer(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[]) {
  enif_fprintf(stdout, "*************************** force destroying consumer \n");
  pulsar_consumer * p_consumer;
  if (!enif_get_resource(env, argv[0], nif_pulsar_consumer_type, (void ** ) & p_consumer)) {
    return make_error_tuple(env,
      "couldn't retrieve consumer resource from given reference");
  }

  if (p_consumer -> consumer == NULL) {
    return make_error_tuple(env, "passed-in a destroyed consumer");
  }

  pulsar_consumer_close(p_consumer -> consumer);
  pulsar_consumer_free(p_consumer -> consumer);
  p_consumer -> consumer = NULL;

  return ATOMS.atomOk;
}

void maybe_set_message_options(ErlNifEnv * env, pulsar_message_t * message, ERL_NIF_TERM map) {
  unsigned long deliver_after_ms;
  ERL_NIF_TERM deliver_after_ms_term;
  if (enif_get_map_value(env, map, ATOMS.atomDeliverAfterMS, & deliver_after_ms_term) &&
    enif_get_uint64(env, deliver_after_ms_term, & deliver_after_ms)) {
    pulsar_message_set_deliver_after(message, (uint64_t) deliver_after_ms);
  }

  unsigned long deliver_at_ms;
  ERL_NIF_TERM deliver_at_ms_term;
  if (enif_get_map_value(env, map, ATOMS.atomDeliverAtMS, & deliver_at_ms_term) &&
    enif_get_uint64(env, deliver_at_ms_term, & deliver_at_ms)) {
    pulsar_message_set_deliver_at(message, (uint64_t) deliver_at_ms);
  }

  unsigned long event_ts;
  ERL_NIF_TERM event_ts_term;
  if (enif_get_map_value(env, map, ATOMS.atomEventTS, & event_ts_term) &&
    enif_get_uint64(env, event_ts_term, & event_ts)) {
    pulsar_message_set_event_timestamp(message, (uint64_t) event_ts);
  }

  ErlNifBinary partition_bin;
  ERL_NIF_TERM partition_key_term;
  if (enif_get_map_value(env, map, ATOMS.atomPartitionKey, & partition_key_term) &&
    enif_inspect_binary(env, partition_key_term, & partition_bin)) {
    const char * partition_key =
      strndup((char * ) partition_bin.data, partition_bin.size);
    pulsar_message_set_partition_key(message, partition_key);
  }

  ERL_NIF_TERM properties_term;
  if (enif_get_map_value(env, map, ATOMS.atomProperties, & properties_term) &&
    enif_is_map(env, properties_term)) {
    ERL_NIF_TERM key_term, val_term;
    ErlNifMapIterator iter;
    enif_map_iterator_create(env, properties_term, & iter,
      ERL_NIF_MAP_ITERATOR_FIRST);

    while (enif_map_iterator_get_pair(env, & iter, & key_term, & val_term)) {
      ErlNifBinary key_bin;
      ErlNifBinary val_bin;
      if (enif_inspect_binary(env, key_term, & key_bin) &&
        enif_inspect_binary(env, val_term, & val_bin)) {
        const char * key_str =
          strndup((char * ) key_bin.data, key_bin.size);
        const char * val_str =
          strndup((char * ) val_bin.data, val_bin.size);
        pulsar_message_set_property(message, key_str, val_str);
      }
      enif_map_iterator_next(env, & iter);
    }
    enif_map_iterator_destroy(env, & iter);
  }
}

/*
	ERL_NIF_TERM
	sync_produce(ErlNifEnv * env, int argc,
		const ERL_NIF_TERM argv[]) {
		pulsar_client * p_client;
		if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void ** ) & p_client)) {
			return make_error_tuple(env,
				"couldn't retrieve client resource from given reference");
		}

		if (p_client -> client == NULL) {
			return make_error_tuple(env, "passed-in a destroyed client");
		}

		ErlNifBinary topic_bin;

		int topic_ret = enif_inspect_binary(env, argv[1], & topic_bin);
		if (!topic_ret) {
			return make_error_tuple(env,
				"failed to create topic binary from input");
		}

		const char * topic_str = strndup((char * ) topic_bin.data, topic_bin.size);

		ErlNifBinary msg_bin;

		int msg_ret = enif_inspect_binary(env, argv[2], & msg_bin);
		if (!msg_ret) {
			return make_error_tuple(env,
				"failed to create message binary from input");
		}

		const char * msg_str = strndup((char * ) msg_bin.data, msg_bin.size);

		pulsar_producer_configuration_t * producer_conf =
			pulsar_producer_configuration_create();
		pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);

		pulsar_producer_t * producer;

		pulsar_result err =
			pulsar_client_create_producer(p_client -> client, topic_str, producer_conf, &
				producer);

		pulsar_producer_configuration_free(producer_conf);

		if (err != pulsar_result_Ok) {
			return make_error_tuple(env, "failed to make pulsar producer");
		}

		pulsar_message_t * message = pulsar_message_create();
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
*/

static void delivery_callback(pulsar_result result, pulsar_message_id_t * msg_id, void * ctx) {
  delivery_callback_ctx deliv_cb_ctx = * (delivery_callback_ctx * ) ctx;
  ErlNifPid actual_pid = deliv_cb_ctx.callback_pid;

	ErlNifEnv * env = enif_alloc_env();

  pulsar_msg_id * p_msg_id;
  p_msg_id =
    enif_alloc_resource(nif_pulsar_msg_id_type, sizeof(pulsar_msg_id));

  p_msg_id -> msg_id = NULL;
  p_msg_id -> msg_id = msg_id;

  ERL_NIF_TERM p_msg_id_res = enif_make_resource(env, p_msg_id);
	enif_release_resource(p_msg_id);

  /* char * p_message_id_str = pulsar_message_id_str(msg_id); */
  /* ERL_NIF_TERM ret_bin = */
  /*   make_binary(env, p_message_id_str, strlen(p_message_id_str)); */

  if (result == pulsar_result_Ok) {
    enif_send(NULL, & actual_pid, env,
      enif_make_tuple5(env, ATOMS.atomDelivery, ATOMS.atomOk,
      p_msg_id_res,  deliv_cb_ctx.payload, deliv_cb_ctx.options));
  } else {
    enif_send(NULL, & actual_pid, env,
      enif_make_tuple5(env, ATOMS.atomDelivery, ATOMS.atomError,
    p_msg_id_res,   deliv_cb_ctx.payload, deliv_cb_ctx.options));
  }

  enif_free_env(env);
}

ERL_NIF_TERM
create_async_producer(ErlNifEnv * env, int argc,  const ERL_NIF_TERM argv[]) {
					pulsar_client * p_client;
					if (!enif_get_resource(env, argv[0], nif_pulsar_client_type, (void ** ) & p_client)) {
						return make_error_tuple(env,
							"couldn't retrieve client resource from given reference");
					}

					if (p_client -> client == NULL) {
						return make_error_tuple(env, "passed-in a destroyed client");
					}

					ErlNifBinary topic_bin;

					int topic_ret = enif_inspect_binary(env, argv[1], & topic_bin);
					if (!topic_ret) {
						return make_error_tuple(env,
							"failed to create topic binary from input");
					}

					const char * topic_str = strndup((char * ) topic_bin.data, topic_bin.size);

					ErlNifPid send_back_to_pid;
					if (!enif_get_local_pid(env, argv[2], & send_back_to_pid)) {
						return make_error_tuple(env,
							"failed to make pulsar producer callback pid");
					}

					pulsar_producer_configuration_t * producer_conf =
						pulsar_producer_configuration_create();
					pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);

						//TODO
          /*
						pulsar_producer_configuration_set_producer_name
						pulsar_producer_configuration_set_send_timeout
						pulsar_producer_configuration_set_initial_sequence_id
						pulsar_producer_configuration_set_compression_type
						pulsar_producer_configuration_set_max_pending_messages
						pulsar_producer_configuration_set_max_pending_messages_across_partitions
						pulsar_producer_configuration_set_partitions_routing_mode
						pulsar_producer_configuration_set_message_router
						pulsar_producer_configuration_set_hashing_scheme
						pulsar_producer_configuration_set_block_if_queue_full
						pulsar_producer_configuration_set_batching_enabled
						pulsar_producer_configuration_set_batching_max_messages
						pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes
						pulsar_producer_configuration_set_batching_max_publish_delay_ms
						pulsar_producer_configuration_set_property
          */


					pulsar_producer_t * producer;

					pulsar_result err =
						pulsar_client_create_producer(p_client -> client, topic_str, producer_conf, &
							producer);

					pulsar_producer_configuration_free(producer_conf);

					if (err != pulsar_result_Ok) {
						return make_error_tuple(env, "failed to make pulsar producer");
					}

					pulsar_producer * p_producer;

					p_producer =
						enif_alloc_resource(nif_pulsar_producer_type, sizeof(pulsar_producer));
					if (!p_producer) {
						return make_error_tuple(env, "no_memory for creating pulsar producer");
					}

					p_producer -> producer = NULL;
					p_producer -> producer = producer;
					p_producer -> callback_pid = send_back_to_pid;

					ERL_NIF_TERM p_producer_res = enif_make_resource(env, p_producer);
					enif_release_resource(p_producer);

					return enif_make_tuple2(env, ATOMS.atomOk, p_producer_res);
}

ERL_NIF_TERM async_produce(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[]) {
  pulsar_producer * p_producer;
  if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void ** ) & p_producer)) {
    return make_error_tuple(env,
      "couldn't retrieve producer resource from given reference");
  }

  if (p_producer -> producer == NULL) {
    return make_error_tuple(env, "passed-in a destroyed producer");
  }

  ErlNifBinary msg_bin;

  int msg_ret = enif_inspect_binary(env, argv[1], & msg_bin);
  if (!msg_ret) {
    return make_error_tuple(env,
      "failed to create message binary from input");
  }

  const char * msg_str = strndup((char * ) msg_bin.data, msg_bin.size);

  pulsar_message_t * message = pulsar_message_create();
  pulsar_message_set_content(message, msg_str, strlen(msg_str));
  maybe_set_message_options(env, message, argv[2]);

  delivery_callback_ctx * delivery_cb_ctx;
  delivery_cb_ctx =
    enif_alloc_resource(nif_delivery_callback_ctx_type,
      sizeof(delivery_callback_ctx));
  if (!delivery_cb_ctx) {
    return make_error_tuple(env,
      "no_memory for creating delivery callback context");
  }

  delivery_cb_ctx -> callback_pid = p_producer -> callback_pid;
  delivery_cb_ctx -> payload = argv[1];
  delivery_cb_ctx -> options = argv[2];

  pulsar_producer_send_async(p_producer -> producer, message,
    delivery_callback, delivery_cb_ctx);

	enif_release_resource(delivery_cb_ctx);
  pulsar_message_free(message);

  return ATOMS.atomOk;
}

ERL_NIF_TERM destroy_producer(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[]) {
  enif_fprintf(stdout, "*************************** force destroying producer \n");
  pulsar_producer * p_producer;
  if (!enif_get_resource(env, argv[0], nif_pulsar_producer_type, (void ** ) & p_producer)) {
    return make_error_tuple(env,
      "couldn't retrieve producer resource from given reference");
  }

  if (p_producer -> producer == NULL) {
    return make_error_tuple(env, "passed-in a destroyed producer");
  }

  pulsar_producer_flush(p_producer->producer);
  pulsar_producer_close(p_producer -> producer);
  pulsar_producer_free(p_producer -> producer);
  p_producer -> producer = NULL;

  return ATOMS.atomOk;
}

/*
 *Below is used for nif lifecycle
 */
static int
on_load(ErlNifEnv * env, void ** priv, ERL_NIF_TERM info) {
  ATOMS.atomOk = make_atom(env, "ok");
  ATOMS.atomError = make_atom(env, "error");
  ATOMS.atomDelivery = make_atom(env, "delivery_callback");
  ATOMS.atomNeutronMsg = make_atom(env, "neutron_msg");
  ATOMS.atomUrl = make_atom(env, "url");
  ATOMS.atomIOThreads = make_atom(env, "io_threads");
  ATOMS.atomMsgListenerThreads = make_atom(env, "msg_listener_threads");
  ATOMS.atomSendBackToPid = make_atom(env, "send_back_to_pid");
  ATOMS.atomSubscription = make_atom(env, "subscription");
  ATOMS.atomTopic = make_atom(env, "topic");
  ATOMS.atomTypeInt = make_atom(env, "type_int");
  ATOMS.atomDeliverAfterMS = make_atom(env, "deliver_after_ms");
  ATOMS.atomDeliverAtMS = make_atom(env, "deliver_at_ms");
  ATOMS.atomEventTS = make_atom(env, "event_ts");
  ATOMS.atomPublishTS = make_atom(env, "publish_ts");
  ATOMS.atomPartitionKey = make_atom(env, "partition_key");
  ATOMS.atomProperties = make_atom(env, "properties");
  ATOMS.atomReceiverQueueSize = make_atom(env, "receiver_queue_size");
  ATOMS.atomMaxTotalReceiverQueueSizeAcrossPartitions =
    make_atom(env, "max_total_receiver_queue_size_across_partitions");
  ATOMS.atomConsumerName = make_atom(env, "consumer_name");
  ATOMS.atomUnackedMessagesTimeoutMS =
    make_atom(env, "unacked_messages_timeout_ms");
  ATOMS.atomNegativeAckRedeliveryDelayMS =
    make_atom(env, "negative_ack_redelivery_delay_ms");
  ATOMS.atomAckGroupingTimeMS = make_atom(env, "ack_grouping_time_ms");
  ATOMS.atomAckGroupingMaxSize = make_atom(env, "ack_grouping_max_size");
  ATOMS.atomReadCompacted = make_atom(env, "read_compacted");
  ATOMS.atomSubscriptionInitialPosition =
    make_atom(env, "subscription_initial_position");
  ATOMS.atomOperationTimeoutSeconds =
    make_atom(env, "operation_timeout_seconds");
  ATOMS.atomConcurrentLookupRequest =
    make_atom(env, "concurrent_lookup_request");
  ATOMS.atomStatsIntervalInSeconds =
    make_atom(env, "stats_interval_in_seconds");

  ErlNifResourceType * rt_client;
  ErlNifResourceType * rt_consumer;
  ErlNifResourceType * rt_msg_id;
  ErlNifResourceType * rt_producer;
  ErlNifResourceType * rt_delivery_cb_ctx;

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
on_reload(ErlNifEnv * env, void ** priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static int
on_upgrade(ErlNifEnv * env, void ** priv, void ** old_priv_data,
  ERL_NIF_TERM load_info) {
  return 0;
}

ErlNifFunc nif_funcs[] = {
  /* { */
  /*   "sync_produce", */
  /*   4, */
  /*   sync_produce, */
  /*   ERL_NIF_DIRTY_JOB_IO_BOUND */
  /* }, */
  {
    "make_client",
    1,
    make_client,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "destroy_client",
    1,
    destroy_client,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "do_consume",
    2,
    do_consume,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "ack",
    2,
    ack,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "ack_all",
    2,
    ack_all,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "nack",
    2,
    nack
  },
  {
    "destroy_consumer",
    1,
    destroy_consumer,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "create_async_producer",
    3,
    create_async_producer,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
  {
    "async_produce",
    3,
    async_produce
  },
  {
    "destroy_producer",
    1,
    destroy_producer,
    ERL_NIF_DIRTY_JOB_IO_BOUND
  },
};

ERL_NIF_INIT(Elixir.Neutron.PulsarNifs, nif_funcs, on_load, on_reload, on_upgrade, NULL)
