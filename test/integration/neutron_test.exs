defmodule NeutronTest do
  use ExUnit.Case
  use ExUnitProperties
  use Divo, services: [:pulsar]

  property "sync produce is always successful" do
    check all(
            message <- binary(),
            partition_key <- binary()
          ) do
      assert :ok ==
               Neutron.sync_produce("my-topic-produce", message, partition_key: partition_key)
    end
  end

  test "async produce is always successful" do
    defmodule DeliverCallback do
      @behaviour Neutron.PulsarAsyncProducerCallback
      @compiled_pid :erlang.pid_to_list(self())

      @impl true
      def handle_delivery(res) do
        send(:erlang.list_to_pid(@compiled_pid), res)
      end
    end

    message = "hello test deliver"

    {:ok, pid} = Neutron.create_async_producer("my-topic-async-produce", DeliverCallback)

    :ok =
      Neutron.async_produce(pid, message, partition_key: UUID.uuid4(), properties: %{test: true})

    assert_receive {:ok, _new_msg_id, ^message}
  end

  test "async iodata produce is always successful when data is iodata" do
    defmodule DeliverIoDataCallback do
      @behaviour Neutron.PulsarAsyncProducerCallback
      @compiled_pid :erlang.pid_to_list(self())

      @impl true
      def handle_delivery(res) do
        _msg = send(:erlang.list_to_pid(@compiled_pid), {:test_deliver, res})
      end
    end

    # hello test driver
    io_message = [<<104,101,108,108,111,32,116,101,115,116,32,100,114,105,118,101,114>>]
    message = IO.iodata_to_binary(io_message)

    {:ok, pid} = Neutron.create_async_producer("io-topic-async-produce", DeliverIoDataCallback)

    :ok =
      Neutron.async_iodata_produce!(pid, io_message, partition_key: UUID.uuid4(), properties: %{test: true})

    assert_receive {:test_deliver, {:ok, _new_msg_id, ^message}}
  end

  test "async iodata produce is always successful when data is binary" do
    defmodule DeliverIoDataBinCallback do
      @behaviour Neutron.PulsarAsyncProducerCallback
      @compiled_pid :erlang.pid_to_list(self())

      @impl true
      def handle_delivery(res) do
        send(:erlang.list_to_pid(@compiled_pid), res)
      end
    end

    # hello test driver
    message = "hello test driver"

    {:ok, pid} = Neutron.create_async_producer("iobin-topic-async-produce", DeliverIoDataBinCallback)

    :ok =
      Neutron.async_iodata_produce!(pid, message, partition_key: UUID.uuid4(), properties: %{test: true})

    assert_receive {:ok, _new_msg_id, ^message}
  end

  test "sync produce and consume roundtrip" do
    defmodule ConsumerCallback do
      @behaviour Neutron.PulsarConsumerCallback
      @compiled_pid :erlang.pid_to_list(self())

      @impl true
      def handle_message(
            {:neutron_msg, topic, _msg_id, partition_key, _publish_ts, event_ts,
             _redelivery_count, properties, payload},
            _state
          ) do
        send(
          :erlang.list_to_pid(@compiled_pid),
          {:test_callback, topic, partition_key, event_ts, properties, payload}
        )

        Process.sleep(10)
        :ack
      end
    end

    partition_key = UUID.uuid4()
    event_ts = :rand.uniform(10000)
    id = UUID.uuid4()
    action = "index"
    properties = %{"id" => id, "action" => action}
    payload = UUID.uuid4()

    topic = "persistent://public/default/my-topic-consume"

    {:ok, _pid} =
      Neutron.start_consumer(
        callback_module: ConsumerCallback,
        topic: topic,
        receiver_queue_size: 1000,
        max_total_receiver_queue_size_across_partitions: 1001,
        consumer_name: "consumer_name",
        unacked_messages_timeout_ms: 10_002,
        negative_ack_redelivery_delay_ms: 1003,
        ack_grouping_time_ms: 1004,
        ack_grouping_max_size: 1005,
        read_compacted: false,
        subscription_initial_position: :latest,
        properties: %{"test" => "true"}
      )

    :ok =
      Neutron.sync_produce(topic, payload,
        partition_key: partition_key,
        event_ts: event_ts,
        properties: properties
      )

    assert_receive {:test_callback, ^topic, ^partition_key, ^event_ts, ^properties, ^payload}
  end

  test "sync produce delay_after and consume roundtrip" do
    defmodule ConsumerDelayAfterCallback do
      @behaviour Neutron.PulsarConsumerCallback
      @delay_ms 2_000

      def delay, do: @delay_ms

      @impl true
      def handle_message(
            {:neutron_msg, _topic, _msg_id, _partition_key, _publish_ts, _event_ts,
             _redelivery_count, _properties, payload},
            _state
          ) do
        old_time_unix_ms = String.to_integer(payload)
        now_unix_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)

        # the default tick is 1 second so we give a 1 second buffer
        assert now_unix_ms - old_time_unix_ms >= @delay_ms - 1_000
        :ack
      end
    end

    message = "#{DateTime.to_unix(DateTime.utc_now(), :millisecond)}"
    topic = "my-topic-delay-after-consume"

    {:ok, _pid} =
      Neutron.start_consumer(callback_module: ConsumerDelayAfterCallback, topic: topic)

    :ok =
      Neutron.sync_produce(topic, message, deliver_after_ms: ConsumerDelayAfterCallback.delay())

    Process.sleep(2_005)
  end

  test "sync produce delay_at and consume roundtrip" do
    defmodule ConsumerDelayAtCallback do
      @behaviour Neutron.PulsarConsumerCallback
      @delay_ms 2_000

      def delay, do: @delay_ms

      @impl true
      def handle_message(
            {:neutron_msg, _topic, _msg_id, _partition_key, _publish_ts, _event_ts,
             _redelivery_count, _properties, payload},
            _state
          ) do
        old_time_unix_ms = String.to_integer(payload)
        now_unix_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)

        # the default tick is 1 second so we give a 1 second buffer
        assert now_unix_ms - old_time_unix_ms >= @delay_ms - 1_000
        :ack
      end
    end

    topic = "my-topic-delay-at-consume"
    {:ok, _pid} = Neutron.start_consumer(callback_module: ConsumerDelayAtCallback, topic: topic)

    current_time_unix_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)
    message = "#{current_time_unix_ms}"
    unix_time_to_send_ms = current_time_unix_ms + ConsumerDelayAtCallback.delay()
    :ok = Neutron.sync_produce(topic, message, deliver_at_ms: unix_time_to_send_ms)
    Process.sleep(2_005)
  end
end
