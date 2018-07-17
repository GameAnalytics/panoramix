defmodule ElixirDruidTest do
  use ExUnit.Case
  doctest ElixirDruid
  require ElixirDruid.Query

  test "builds a query" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day
    assert is_binary(ElixirDruid.Query.to_json(query))
    #IO.puts ElixirDruid.Query.to_json(query)
  end

  test "builds a query with an aggregator" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregators: [event_count: count(),
                    unique_ids: hyperUnique(:user_unique)]
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["aggregators"] == [%{"name" => "event_count",
                                        "type" => "count"},
                                      %{"name" => "unique_ids",
                                        "type" => "hyperUnique",
                                        "fieldName" => "user_unique"}]
    #IO.puts json
  end

  test "set an aggregator after building the query" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day
    query = query |>
      ElixirDruid.Query.set(aggregators: [event_count: count()])
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["aggregators"] == [%{"name" => "event_count",
                                        "type" => "count"}]
    #IO.puts json
  end

  test "build query with column comparison filter" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == dimensions["bar"]
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "columnComparison",
                                  "dimensions" => ["foo", "bar"]}
  end

  test "build query with selector filter" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar"
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "selector",
                                  "dimension" => "foo",
                                  "value" => "bar"}
  end

  test "build query with two filters ANDed together" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar" and dimensions.bar == dimensions.foo
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "and",
                                  "fields" =>
                                    [%{"type" => "selector",
                                       "dimension" => "foo",
                                       "value" => "bar"},
                                     %{"type" => "columnComparison",
                                       "dimensions" => ["bar", "foo"]}]}
  end

  test "build query with two filters ORed together" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar" or dimensions.bar == dimensions.foo
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "or",
                                  "fields" =>
                                    [%{"type" => "selector",
                                       "dimension" => "foo",
                                       "value" => "bar"},
                                     %{"type" => "columnComparison",
                                       "dimensions" => ["bar", "foo"]}]}
  end

  test "build query with a NOT filter" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: not (dimensions.foo == "bar")
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "not",
                                  "field" =>
                                    %{"type" => "selector",
                                      "dimension" => "foo",
                                      "value" => "bar"}}
  end

  test "build query with an 'in' filter" do
    x = "baz"
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo in ["bar", x]
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "in",
                                  "dimension" => "foo",
                                  "values" => ["bar", "baz"]}
  end

  test "build query with a non-strict 'bound' filter" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: 1 <= dimensions.foo <= 10
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "bound",
                                  "dimension" => "foo",
                                  "lower" => "1",
                                  "upper" => "10",
                                  "lowerStrict" => false,
                                  "upperStrict" => false,
                                  "ordering" => "numeric"}
  end

  test "build query with a lower-strict 'bound' filter" do
    query = ElixirDruid.Query.build "timeseries", "my_datasource",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: ("aaa" < dimensions.foo) <= "bbb"
    json = ElixirDruid.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "bound",
                                  "dimension" => "foo",
                                  "lower" => "aaa",
                                  "upper" => "bbb",
                                  "lowerStrict" => true,
                                  "upperStrict" => false,
                                  "ordering" => "lexicographic"}
  end

end
