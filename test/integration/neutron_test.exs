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
    :ok = Neutron.async_produce(pid, message)

    assert_receive {:test_deliver, {:ok, _any}}
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
end
