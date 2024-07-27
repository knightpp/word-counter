defmodule WordCounter do
  use Application

  def start(_type, _args) do
    stream = File.stream!("simplewiki.xml")
    WordCounter.Supervisor.start_link(stream)
  end

  def run(counters \\ 1) do
    WordCounter.CounterSupervisor.add_child(
      WordCounter.CounterSupervisor,
      WordCounter.Parser,
      WordCounter.Accumulator,
      counters
    )

    # stream = File.stream!("simplewiki.xml")
    # WordCounter.Supervisor.start_link(%{stream: stream, counters: counters})
  end
end

defmodule WordCounter.Supervisor do
  require Logger
  use Supervisor

  @impl true
  def init(stream) do
    counter_sup_opts = [
      parser: WordCounter.Parser,
      accumulator: WordCounter.Accumulator
    ]

    children = [
      WordCounter.Accumulator,
      {WordCounter.Parser, stream},
      # TODO: this depends on available global names, is there a way to change that?
      {WordCounter.CounterSupervisor, counter_sup_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_link(arg) do
    Logger.info("Creating #{__MODULE__}")
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
    Logger.info("Creating #{__MODULE__}")
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
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
    Logger.info("Creating #{__MODULE__}")
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
  use DynamicSupervisor

  @impl true
  def init(_arg) do
    DynamicSupervisor.init(strategy: :one_for_one, max_children: 1024)
  end

  def start_link(arg) do
    Logger.info("Creating #{__MODULE__}")
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def add_child(sup, parser, accumulator, children \\ 1) do
    child = {WordCounter.Counter, %{parser: parser, accumulator: accumulator}}

    Stream.repeatedly(fn -> child end)
    |> Stream.take(children)
    |> Enum.each(fn child ->
      {:ok, _} = DynamicSupervisor.start_child(sup, child)
    end)
  end
end

defmodule WordCounter.Parser do
  require Logger
  use GenServer

  def init(stream) do
    {:ok, parser} = spawn_parser(stream)

    {:ok, {[], parser}}
  end

  def start_link(arg) do
    Logger.info("Creating #{__MODULE__}")
    # TODO: how to remove global names?
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
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

  defp spawn_parser(stream) do
    Task.start_link(fn -> Saxy.parse_stream(stream, WordCounter.Parser.Handler, {}) end)
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
