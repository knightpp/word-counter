defmodule WordCounterTest do
  use ExUnit.Case
  doctest WordCounter

  test "greets the world" do
    assert WordCounter.hello() == :world
  end
end
