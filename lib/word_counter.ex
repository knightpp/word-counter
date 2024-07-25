defmodule WordCounter.Parser do
  use GenServer

  def init(stream) do
    {:ok, parser} =
      Task.start_link(fn -> Saxy.parse_stream(stream, WordCounter.Parser.Handler, {}) end)

    {:ok, {[], parser}}
  end

  def demand_page(server) do
    GenServer.cast(server, {:demand_page, self()})
  end

  def wait_for_page() do
    receive do
      {:page_ready, content} -> content
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
      IO.puts("waiting for demand")

      receive do
        {:demand_page, from} -> send(from, {:page_ready, content})
      end

      IO.puts("got demand, processing next page")

      {:ok, state}
    else
      {:ok, state}
    end
  end
end
