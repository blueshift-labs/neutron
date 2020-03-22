defmodule NeutronTest do
  use ExUnit.Case
  doctest Neutron

  test "greets the world" do
    assert Neutron.hello() == :world
  end
end
