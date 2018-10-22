defmodule ElixirDruid.Error do
  defexception [:message]
  @type t :: %__MODULE__{}
end

defmodule ElixirDruid do

  @moduledoc """
  Documentation for ElixirDruid.
  """

  @spec post_query(%ElixirDruid.Query{}, atom()) :: {:ok, term()} |
  {:error, HTTPoison.Error.t() | Jason.DecodeError.t() | ElixirDruid.Error.t()}
  def post_query(query, profile \\ :default) do
    url_path = "/druid/v2"
    body = ElixirDruid.Query.to_json query
    headers = [{"Content-Type", "application/json"}]

    request_and_decode(profile, :post, url_path, body, headers)
  end

  @spec post_query!(%ElixirDruid.Query{}, atom()) :: term()
  def post_query!(query, profile \\ :default) do
    case post_query(query, profile) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @spec status(atom) :: {:ok, term()} |
  {:error, HTTPoison.Error.t() | Jason.DecodeError.t() | ElixirDruid.Error.t()}
  def status(profile) do
    url_path = "/status"
    body = ""
    headers = []

    request_and_decode(profile, :get, url_path, body, headers)
  end

  @spec status!(atom) :: term()
  def status!(profile) do
    case status(profile) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  defp request_and_decode(profile, method, url_path, body, headers) do
    broker_profiles = Application.get_env(:elixir_druid, :broker_profiles)
    broker_profile = broker_profiles[profile] ||
      raise ArgumentError, "no broker profile with name #{profile}"
    url = broker_profile[:base_url] <> url_path
    options = http_options(url, broker_profile)

    with {:ok, http_response} <-
	 HTTPoison.request(method, url, body, headers, options),
	 {:ok, body} <- maybe_handle_druid_error(http_response),
	 {:ok, decoded} <- Jason.decode body do
	   {:ok, decoded}
	 end
  end

  defp http_options(url, broker_profile) do
    ssl_options(url, broker_profile) ++ auth_options(broker_profile) ++ timeout_options()
  end

  defp ssl_options(url, broker_profile) do
    if url =~ ~r(^https://) do
      cacertfile = broker_profile[:cacertfile]
      [ssl: [verify: :verify_peer, cacertfile: cacertfile, depth: 10]]
    else
      []
    end
  end

  defp auth_options(broker_profile) do
    if broker_profile[:http_username] do
      auth = {broker_profile[:http_username], broker_profile[:http_password]}
      [hackney: [basic_auth: auth]]
    else
      []
    end
  end

  defp timeout_options() do
    # Default to 120 seconds
    request_timeout = Application.get_env(:elixir_druid, :request_timeout, 120_000)
    [recv_timeout: request_timeout]
  end

  defp maybe_handle_druid_error(
    %HTTPoison.Response{status_code: 200, body: body}) do
    {:ok, body}
  end
  defp maybe_handle_druid_error(
    %HTTPoison.Response{status_code: status_code, body: body}) do
    message =
      "Druid error (code #{status_code}): " <>
      case Jason.decode body do
        {:ok, %{"error" => _} = decoded} ->
	  # Usually we'll get a JSON object from Druid with "error",
	  # "errorMessage", "errorClass" and "host".  Some of them
          # might be null.
          Enum.join(
            for field <- ["error", "errorMessage", "errorClass", "host"],
            decoded[field] do
              "#{field}: #{decoded[field]}"
            end, " ")
        _ ->
	  "undecodable error: " <> body
      end
    {:error, %ElixirDruid.Error{message: message}}
  end

  @doc ~S"""
  Format a date or a datetime into a format that Druid expects.

  ## Examples

      iex> ElixirDruid.format_time! ~D[2018-07-20]
      "2018-07-20"
      iex> ElixirDruid.format_time!(
      ...>   Timex.to_datetime({{2018,07,20},{1,2,3}}))
      "2018-07-20T01:02:03+00:00"
  """
  def format_time!(%DateTime{} = datetime) do
    Timex.format! datetime, "{ISO:Extended}"
  end
  def format_time!(%Date{} = date) do
    Timex.format! date, "{ISOdate}"
  end

  defmacro __using__(_params) do
    quote do
      import ElixirDruid.Query, only: [from: 2]
    end
  end

end
