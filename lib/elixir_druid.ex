defmodule ElixirDruid.Error do
  defexception [:message]
  @type t :: %__MODULE__{}
end

defmodule ElixirDruid do

  @moduledoc """
  Documentation for ElixirDruid.
  """

  @spec post_query(atom, %ElixirDruid.Query{}) :: {:ok, term()} |
  {:error, HTTPoison.Error.t() | Jason.DecodeError.t() | ElixirDruid.Error.t()}
  def post_query(profile, query) do
    url_path = "/druid/v2"
    body = ElixirDruid.Query.to_json query
    headers = [{"Content-Type", "application/json"}]

    request_and_decode(profile, :post, url_path, body, headers)
  end

  @spec post_query!(atom, %ElixirDruid.Query{}) :: term()
  def post_query!(profile, query) do
    case post_query(profile, query) do
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
    broker_profile = broker_profiles[profile]
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
    ssl_options(url, broker_profile) ++ auth_options(broker_profile)
  end

  defp ssl_options(url, broker_profile) do
    if url =~ ~r(^https://) do
      cacertfile = broker_profile[:cacertfile]
      [ssl: [verify: :verify_peer, cacertfile: cacertfile]]
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

  defp maybe_handle_druid_error(
    %HTTPoison.Response{status_code: 200, body: body}) do
    {:ok, body}
  end
  defp maybe_handle_druid_error(
    %HTTPoison.Response{status_code: status_code, body: body}) do
    message =
      case Jason.decode body do
	{:ok, %{"error" => error}} ->
	  # Sometimes body is a JSON object with an error field
	  "Druid error (code #{status_code}): #{error}"
	_ ->
	  "Druid error (code #{status_code})"
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
