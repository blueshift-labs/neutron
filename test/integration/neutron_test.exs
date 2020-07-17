defmodule NeutronTest do
  use ExUnit.Case
  use ExUnitProperties
  use Divo, services: [:pulsar]

  property "sync produce is always successful" do
    check all(message <- binary()) do
      assert :ok == Neutron.sync_produce("my-topic-produce", message)
    end
  end

  test "async produce is always successful" do
    defmodule DeliverCallback do
      @behaviour Neutron.PulsarAsyncProducerCallback
      @compiled_pid self()

      @impl true
      def handle_delivery(res) do
        _msg = send(@compiled_pid, {:test_deliver, res})
      end
    end

    message = "hello test deliver"

    {:ok, pid} = Neutron.create_async_producer("my-topic-produce", DeliverCallback)
    {:ok, msg_id} = Neutron.async_produce(pid, message)

    assert_receive {:test_deliver, {:ok, ^msg_id}}
  end

  test "sync produce and consume roundtrip" do
    defmodule ConsumerCallback do
      @behaviour Neutron.PulsarConsumerCallback
      @compiled_pid self()

      @impl true
      def handle_message(msg) do
        _msg = send(@compiled_pid, {:test_callback, msg})
        Process.sleep(10)
        :ack
      end
    end

    message = "hello test consume"
    topic = "my-topic-consume"

    {:ok, _pid} = Neutron.start_consumer(callback_module: ConsumerCallback, topic: topic)
    :ok = Neutron.sync_produce(topic, message)

    assert_receive {:test_callback, message}
  end

  test "sync produce delay_after and consume roundtrip" do
    defmodule ConsumerCallback do
      @behaviour Neutron.PulsarConsumerCallback
      @delay_ms 2_000

      def delay, do: @delay_ms

      @impl true
      def handle_message(msg) do
        old_time_unix_ms = String.to_integer(msg)
        now_unix_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)

        # the default tick is 1 second so we give a 1 second buffer
        assert now_unix_ms - old_time_unix_ms >= @delay_ms - 1_000
        :ack
      end
    end

    message = "#{DateTime.to_unix(DateTime.utc_now(), :millisecond)}"
    topic = "my-topic-consume"

    {:ok, _pid} = Neutron.start_consumer(callback_module: ConsumerCallback, topic: topic)
    :ok = Neutron.sync_produce(topic, message, %{deliver_after_ms: ConsumerCallback.delay()})
    Process.sleep(2_005)
  end

  test "sync produce delay_at and consume roundtrip" do
    defmodule ConsumerCallback do
      @behaviour Neutron.PulsarConsumerCallback
      @delay_ms 2_000

      def delay, do: @delay_ms

      @impl true
      def handle_message(msg) do
        old_time_unix_ms = String.to_integer(msg)
        now_unix_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)

        # the default tick is 1 second so we give a 1 second buffer
        assert now_unix_ms - old_time_unix_ms >= @delay_ms - 1_000
        :ack
      end
    end

    topic = "my-topic-consume"
    {:ok, _pid} = Neutron.start_consumer(callback_module: ConsumerCallback, topic: topic)

    current_time_unix_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)
    message = "#{current_time_unix_ms}"
    unix_time_to_send_ms = current_time_unix_ms + ConsumerCallback.delay()
    :ok = Neutron.sync_produce(topic, message, %{deliver_at_ms: unix_time_to_send_ms})
    Process.sleep(2_005)
  end
end
