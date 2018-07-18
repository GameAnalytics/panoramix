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
    broker_profiles = Application.get_env(:elixir_druid, :broker_profiles)
    broker_profile = broker_profiles[profile]
    url = broker_profile[:base_url] <> "/druid/v2"

    options = http_options(url, broker_profile)

    body = ElixirDruid.Query.to_json query
    headers = [{"Content-Type", "application/json"}]

    request_and_decode(:post, url, body, headers, options)
  end

  @spec status(atom) :: {:ok, term()} |
  {:error, HTTPoison.Error.t() | Jason.DecodeError.t() | ElixirDruid.Error.t()}
  def status(profile) do
    broker_profiles = Application.get_env(:elixir_druid, :broker_profiles)
    broker_profile = broker_profiles[profile]
    url = broker_profile[:base_url] <> "/status"

    options = http_options(url, broker_profile)
    headers = []

    request_and_decode(:get, url, "", headers, options)
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

  defp request_and_decode(method, url, body, headers, options) do
    with {:ok, http_response} <-
	 HTTPoison.request(method, url, body, headers, options),
	 {:ok, body} <- maybe_handle_druid_error(http_response),
	 {:ok, decoded} <- Jason.decode body do
	   {:ok, decoded}
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
end
