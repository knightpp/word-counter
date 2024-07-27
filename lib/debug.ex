defmodule Benchmark do
  def measure(function) do
    {time, ret} = :timer.tc(function)
    {time / 1_000_000, ret}
  end
end

defmodule Debug do
  def bench_drain() do
    Benchmark.measure(fn ->
      0..8
      |> Enum.map(fn _ -> Task.async(fn -> drain(0) end) end)
      |> Enum.map(fn x -> Task.await(x, 20000) end)
    end)
  end

  def drain(calls \\ 0) do
    WordCounter.Parser.demand_page(WordCounter.Parser)

    if WordCounter.Parser.wait_for_page() == :closed do
      calls
    else
      drain(calls + 1)
    end
  end
end
