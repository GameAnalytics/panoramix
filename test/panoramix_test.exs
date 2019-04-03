defmodule PanoramixTest do
  use ExUnit.Case
  doctest Panoramix
  use Panoramix

  test "builds a query" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day
    assert is_binary(Panoramix.Query.to_json(query))
    #IO.puts Panoramix.Query.to_json(query)
  end

  test "builds a query with an aggregation" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [event_count: count(),
                    unique_ids: hyperUnique(:user_unique)]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["aggregations"] == [%{"name" => "event_count",
                                         "type" => "count"},
                                       %{"name" => "unique_ids",
                                         "type" => "hyperUnique",
                                         "fieldName" => "user_unique"}]
    #IO.puts json
  end

  test "builds a query with an aggregation that has an extra parameter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [event_count: count(),
                    unique_ids: hyperUnique(:user_unique, round: true)]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["aggregations"] == [%{"name" => "event_count",
                                         "type" => "count"},
                                       %{"name" => "unique_ids",
                                         "type" => "hyperUnique",
                                         "fieldName" => "user_unique",
                                         "round" => true}]
    #IO.puts json
  end

  test "builds a query with a filtered aggregation" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [
        event_count: count(),
        interesting_event_count: count() when dimensions.interesting == "true"
      ]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["aggregations"] == [
      %{"name" => "event_count",
        "type" => "count"},
      %{"type" => "filtered",
        "filter" => %{"type" => "selector",
                      "dimension" => "interesting",
                      "value" => "true"},
        # NB: it seems to be correct to put the name on the inner aggregator!
        "aggregator" =>
          %{"name" => "interesting_event_count",
            "type" => "count"}}
    ]
    #IO.puts json
  end

  test "builds a query with a filtered aggregation, but the filter is nil" do
    my_filter = nil
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [
        interesting_event_count: count() when ^my_filter
      ]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    # In this case, there is no need to add a filter to the aggregator
    assert decoded["aggregations"] == [
      %{"name" => "interesting_event_count",
        "type" => "count"}
    ]
    # IO.puts json
  end

  test "set an aggregation after building the query" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day
    query = query |>
      from(aggregations: [event_count: count()])
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["aggregations"] == [%{"name" => "event_count",
                                         "type" => "count"}]
    #IO.puts json
  end

  test "build query with column comparison filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == dimensions["bar"]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "columnComparison",
                                  "dimensions" => ["foo", "bar"]}
  end

  test "build query with selector filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar"
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "selector",
                                  "dimension" => "foo",
                                  "value" => "bar"}
  end

  test "build query with two filters ANDed together" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar" and dimensions.bar == dimensions.foo
    json = Panoramix.Query.to_json(query)
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
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar" or dimensions.bar == dimensions.foo
    json = Panoramix.Query.to_json(query)
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

  test "build query with AND and OR filters" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      # 'and' has higher precedence, so this should get parsed as
      # (foo == "bar" or (bar == foo and baz == 17))
      filter: dimensions.foo == "bar" or dimensions.bar == dimensions.foo and dimensions.baz == 17
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "or",
                                  "fields" =>
                                    [%{"type" => "selector",
                                       "dimension" => "foo",
                                       "value" => "bar"},
                                     %{"type" => "and",
                                       "fields" => [
                                         %{"type" => "columnComparison",
                                           "dimensions" => ["bar", "foo"]},
                                         %{"type" => "selector",
                                           "dimension" => "baz",
                                           "value" => 17}]}]}
  end

  test "build query with a NOT filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: not (dimensions.foo == "bar")
    json = Panoramix.Query.to_json(query)
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
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo in ["bar", x]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "in",
                                  "dimension" => "foo",
                                  "values" => ["bar", "baz"]}
  end

  test "build query with a non-strict 'bound' filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: 1 <= dimensions.foo <= 10
    json = Panoramix.Query.to_json(query)
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
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: "aaa" < dimensions.foo <= "bbb"
    json = Panoramix.Query.to_json(query)
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

  test "build query with an 'expression' filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: expression("foo / 1000 < bar")
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "expression",
                                  "expression" => "foo / 1000 < bar"}
  end

  test "add extra filter to existing query" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar"
    query = from query,
      filter: dimensions.bar == "baz" and ^query.filter
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "and",
                                  "fields" =>
                                    [%{"type" => "selector",
                                       "dimension" => "bar",
                                       "value" => "baz"},
                                     %{"type" => "selector",
                                       "dimension" => "foo",
                                       "value" => "bar"}]}
  end

  test "add extra filter to a 'nil' filter with 'and'" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
    # Adding a new filter here, when there is no existing filter,
    # means that the new filter just gets used as the query filter.
    query = from query,
      filter: dimensions.bar == "baz" and ^query.filter
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert decoded["filter"] == %{"type" => "selector",
                                  "dimension" => "bar",
                                  "value" => "baz"}
  end

  test "cannot add filter to 'nil' filter with 'or'" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
    # It's not meaningful to use the empty filter in an "or" expression
    assert_raise RuntimeError, "right operand to 'or' must not be nil", fn ->
      from query,
        filter: dimensions.bar == "baz" or ^query.filter
    end
    assert_raise RuntimeError, "left operand to 'or' must not be nil", fn ->
      from query,
        filter: ^query.filter or dimensions.bar == "baz"
    end
  end

  test "cannot apply 'not' to 'nil' filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
    # It's not meaningful to use the empty filter in an "or" expression
    assert_raise RuntimeError, "operand to 'not' must not be nil", fn ->
      from query,
        filter: not ^query.filter
    end
  end

  test "extract filter from JSON object" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar"
    json = Panoramix.Query.to_json(query)
    %{"filter" => filter} = Jason.decode! json
    new_query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: ^filter
    assert %{"filter" => %{"type" => "selector",
                           "dimension" => "foo",
                           "value" => "bar"}} = Jason.decode! Panoramix.Query.to_json new_query
  end

  test "build a topN query" do
    query = from "my_datasource",
      query_type: "topN",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      dimension: "foo",
      metric: "size",
      threshold: 10
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "topN",
             "dimension" => "foo",
             "metric" => "size",
             "threshold" => 10} = decoded
  end

  test "build a query with a query context" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      filter: dimensions.foo == "bar",
      context: %{timeout: 0,
                 priority: 100,
                 queryId: "my-unique-query-id",
                 skipEmptyBuckets: true}
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"timeout" => 0,
             "priority" => 100,
             "queryId" => "my-unique-query-id",
             "skipEmptyBuckets" => true} = decoded["context"]
  end

  test "build a query with an arithmetic post-aggregation" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [event_count: count(),
                     unique_ids: hyperUnique(:user_unique)],
      post_aggregations: [
        mean_events_per_user: aggregations.event_count / aggregations["unique_ids"]
      ]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert [%{"type" => "arithmetic",
              "name" => "mean_events_per_user",
              "fn" => "/",
              "fields" => [
                %{"type" => "fieldAccess",
                  "fieldName" => "event_count"},
                %{"type" => "fieldAccess",
                  "fieldName" => "unique_ids"}
              ]}] == decoded["postAggregations"]
  end

  test "build a query with an arithmetic post-aggregation including constant" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [event_count: count(),
                     unique_ids: hyperUnique(:user_unique)],
      post_aggregations: [
        mean_events_per_user_pct: aggregations.event_count / aggregations["unique_ids"] * 100
      ]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert [%{"type" => "arithmetic",
              "name" => "mean_events_per_user_pct",
              "fn" => "*",
              "fields" => [
                %{"type" => "arithmetic",
                  "fn" => "/",
                  "fields" => [
                    %{"type" => "fieldAccess",
                      "fieldName" => "event_count"},
                    %{"type" => "fieldAccess",
                      "fieldName" => "unique_ids"}
                  ]},
                %{"type" => "constant",
                  "value" => 100}]}] == decoded["postAggregations"]
  end

  test "build a query with post-aggregation functions" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      aggregations: [event_count: count(),
                     unique_ids: hyperUnique(:user_unique)],
      post_aggregations: [
        cardinality: hyperUniqueCardinality(:unique_ids),
        greatest: doubleGreatest(:event_count, :unique_ids),
        histogram: buckets(:histogram_data, bucketSize: 42, offset: 17)
      ]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert [%{"type" => "hyperUniqueCardinality",
              "name" => "cardinality",
              "fieldName" => "unique_ids"},
            %{"type" => "doubleGreatest",
              "name" => "greatest",
              "fields" => ["event_count", "unique_ids"]},
            %{"name" => "histogram",
              "type" => "buckets",
              "fieldName" => "histogram_data",
              "bucketSize" => 42,
              "offset" => 17}
           ] == decoded["postAggregations"]
  end

  test "build a segmentMetadata query" do
    query = from "my_datasource",
      query_type: "segmentMetadata",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      to_include: :all,
      merge: true,
      analysis_types: [:cardinality, :minmax]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "segmentMetadata",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
             "dataSource" => "my_datasource",
             "toInclude" => %{"type" => "all"},
             "merge" => true,
             "analysisTypes" => ["cardinality", "minmax"],
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end

  test "build a segmentMetadata query limited to certain columns" do
    query = from "my_datasource",
      query_type: "segmentMetadata",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      to_include: ["foo", "bar"],
      merge: true,
      analysis_types: [:cardinality, :minmax]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "segmentMetadata",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
             "dataSource" => "my_datasource",
             "toInclude" => %{"type" => "list", "columns" => ["foo", "bar"]},
             "merge" => true,
             "analysisTypes" => ["cardinality", "minmax"],
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end

  test "build a query using date structs" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: [{~D[2018-05-29], ~D[2018-06-05]}],
      granularity: :day
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29/2018-06-05"],
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end

  test "build a query using datetime structs" do
    from = Timex.to_datetime {{2018, 5, 29}, {1, 30, 0}}
    to = Timex.to_datetime {{2018, 6, 5}, {18, 0, 0}}
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: [{from, to}],
      granularity: :day
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "intervals" => ["2018-05-29T01:30:00+00:00/2018-06-05T18:00:00+00:00"],
             "granularity" => "day",
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end

  test "build a timeBoundary query with a 'maxTime' bound" do
    query = from "my_datasource",
      query_type: "timeBoundary",
      bound: :maxTime
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "timeBoundary",
             "dataSource" => "my_datasource",
             "bound" => "maxTime",
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end

  test "build a query with a virtual column" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      virtual_columns: [plus_one: expression("foo + 1", :long)],
      aggregations: [plus_one_sum: longSum(:plus_one)]
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
             "virtualColumns" => [%{"name" => "plus_one",
                                    "type" => "expression",
                                    "expression" => "foo + 1",
                                    "outputType" => "LONG"}],
             "aggregations" => [%{"name" => "plus_one_sum",
                                  "type" => "longSum",
                                  "fieldName" => "plus_one"}],
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end

  test "build a query with an interval filter" do
    query = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
      granularity: :day,
      # Let's use all three kinds of intervals we support: strings, dates and datetimes
      filter: dimensions.__time in intervals([
        "2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00",
        {~D[2018-06-05], ~D[2018-06-12]},
        {Timex.to_datetime({{2018, 6, 12}, {1, 30, 0}}), Timex.to_datetime({{2018, 6, 19}, {18, 0, 0}})}])
    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode! json
    assert %{"queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
             "filter" => %{"type" => "interval",
                           "dimension" => "__time",
                           "intervals" => [
                             "2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00",
                             "2018-06-05/2018-06-12",
                             "2018-06-12T01:30:00+00:00/2018-06-19T18:00:00+00:00"]},
             "context" => %{"timeout" => 120_000, "priority" => 0}} == decoded
  end
end
