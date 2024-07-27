defmodule WordCounter do
  def run(counters \\ 1) do
    stream = File.stream!("simplewiki.xml")
    Supervisor.start_link(WordCounter.Supervisor, %{stream: stream, counters: counters})
  end
end

defmodule WordCounter.Supervisor do
  require Logger
  use Supervisor

  @impl true
  def init(%{stream: stream, counters: counters}) do
    counter_sup_opts = [
      parser: WordCounter.Parser,
      accumulator: WordCounter.Accumulator,
      children: counters
    ]

    children = [
      WordCounter.Accumulator,
      {WordCounter.Parser, stream},
      {WordCounter.CounterSupervisor, counter_sup_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_link(arg) do
    Logger.debug("Creating #{__MODULE__}")
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end
end

defmodule WordCounter.Accumulator do
  require Logger
  use GenServer

  def init(_arg) do
    {:ok, %{}}
  end

  def start_link(arg) do
    Logger.debug("Creating #{__MODULE__}")
    GenServer.start_link(__MODULE__, arg)
  end

  @spec append(GenServer.server(), Counts.t()) :: :ok
  def append(server, counts) do
    GenServer.call(server, {:append, counts})
  end

  @spec get(GenServer.server()) :: Counts.t()
  def get(server) do
    GenServer.call(server, :get)
  end

  def handle_call({:append, counts}, _from, acc) do
    {:reply, :ok, Map.merge(counts, acc, fn _, va, vb -> va + vb end)}
  end

  def handle_call(:get, _from, acc) do
    {:reply, acc, acc}
  end
end

defmodule Counts do
  @type t() :: %{String.t() => integer()}
end

defmodule WordCounter.Counter do
  require Logger
  use Task, restart: :permanent

  def start_link(%{parser: parser, accumulator: accumulator}) do
    Logger.debug("Creating #{__MODULE__}")
    Task.start_link(__MODULE__, :loop, [parser, accumulator])
  end

  @spec loop(GenServer.server(), GenServer.server()) :: no_return()
  def loop(parser, accumulator) do
    WordCounter.Parser.demand_page(parser)

    case WordCounter.Parser.wait_for_page() do
      :closed -> exit(:normal)
      page -> count_page(accumulator, page)
    end

    loop(parser, accumulator)
  end

  @spec count_page(GenServer.server(), String.t()) :: :ok
  defp count_page(accumulator, page) do
    counts =
      String.split(page) |> Enum.reduce(%{}, fn x, acc -> Map.update(acc, x, 1, &(&1 + 1)) end)

    WordCounter.Accumulator.append(accumulator, counts)
  end
end

defmodule WordCounter.CounterSupervisor do
  require Logger
  use Supervisor

  @impl true
  def init(arg) do
    parser = Keyword.fetch!(arg, :parser)
    accumulator = Keyword.fetch!(arg, :accumulator)
    children = Keyword.get(arg, :children, 1)

    child = {WordCounter.Counter, %{parser: parser, accumulator: accumulator}}
    children = Stream.repeatedly(fn -> child end) |> Enum.take(children)

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_link(arg) do
    Logger.debug("Creating #{__MODULE__}")
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end
end

defmodule WordCounter.Parser do
  require Logger
  use GenServer

  def init(stream) do
    {:ok, parser} =
      Task.start_link(fn -> Saxy.parse_stream(stream, WordCounter.Parser.Handler, {}) end)

    {:ok, {[], parser}}
  end

  def start_link(arg) do
    Logger.debug("Creating #{__MODULE__}")
    GenServer.start_link(__MODULE__, arg)
  end

  @spec demand_page(GenServer.server()) :: :ok
  def demand_page(server) do
    GenServer.cast(server, {:demand_page, self()})
  end

  @spec wait_for_page() :: :closed | String.t()
  def wait_for_page() do
    receive do
      {:page_ready, content} -> content
      :closed -> :closed
    end
  end

  def handle_cast({:demand_page, from}, {queue, parser}) do
    send(parser, {:demand_page, self()})
    {:noreply, {[from | queue], parser}}
  end

  def handle_info({:page_ready, page}, {[first | rest], parser}) do
    send(first, {:page_ready, page})
    {:noreply, {rest, parser}}
  end
end

defmodule WordCounter.Parser.Handler do
  require Logger
  @behaviour Saxy.Handler

  def handle_event(:start_document, _prolog, state), do: {:ok, state}
  def handle_event(:end_document, _prolog, state), do: {:ok, state}

  def handle_event(:start_element, {name, _attributes}, _state) do
    case name do
      # "page" -> {:ok, :page}
      "text" -> {:ok, :text}
      _ -> {:ok, :unknown}
    end
  end

  def handle_event(:end_element, _name, _state) do
    {:ok, :unknown}
  end

  def handle_event(:characters, content, state) do
    if state == :text do
      Logger.debug("waiting for demand")

      receive do
        {:demand_page, from} -> send(from, {:page_ready, content})
      end

      Logger.debug("got demand, processing next page")

      {:ok, state}
    else
      {:ok, state}
    end
  end
end
