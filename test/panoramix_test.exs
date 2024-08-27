defmodule PanoramixTest do
  use ExUnit.Case
  doctest Panoramix
  use Panoramix

  test "builds a query" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day
      )

    assert is_binary(Panoramix.Query.to_json(query))
    # IO.puts Panoramix.Query.to_json(query)
  end

  test "builds a query with an aggregation" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [event_count: count(), unique_ids: hyperUnique(:user_unique)]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["aggregations"] == [
             %{"name" => "event_count", "type" => "count"},
             %{"name" => "unique_ids", "type" => "hyperUnique", "fieldName" => "user_unique"}
           ]

    # IO.puts json
  end

  test "builds a query with an aggregation that has an extra parameter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [event_count: count(), unique_ids: hyperUnique(:user_unique, round: true)]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["aggregations"] == [
             %{"name" => "event_count", "type" => "count"},
             %{
               "name" => "unique_ids",
               "type" => "hyperUnique",
               "fieldName" => "user_unique",
               "round" => true
             }
           ]

    # IO.puts json
  end

  test "builds a query with an aggregation type that needs a name normalization" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [
          event_count: count(),
          unique_ids: hllSketchMerge(:user_unique, round: true)
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["aggregations"] == [
             %{"name" => "event_count", "type" => "count"},
             %{
               "name" => "unique_ids",
               "type" => "HLLSketchMerge",
               "fieldName" => "user_unique",
               "round" => true
             }
           ]

    # IO.puts json
  end

  test "builds a query with a filtered aggregation" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [
          event_count: count(),
          interesting_event_count: count() when dimensions.interesting == "true"
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["aggregations"] == [
             %{"name" => "event_count", "type" => "count"},
             %{
               "type" => "filtered",
               "filter" => %{
                 "type" => "selector",
                 "dimension" => "interesting",
                 "value" => "true"
               },
               # NB: it seems to be correct to put the name on the inner aggregator!
               "aggregator" => %{"name" => "interesting_event_count", "type" => "count"}
             }
           ]

    # IO.puts json
  end

  test "builds a query with a filtered aggregation, but the filter is nil" do
    my_filter = nil

    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [
          interesting_event_count: count() when ^my_filter
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)
    # In this case, there is no need to add a filter to the aggregator
    assert decoded["aggregations"] == [
             %{"name" => "interesting_event_count", "type" => "count"}
           ]

    # IO.puts json
  end

  test "set an aggregation after building the query" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day
      )

    query =
      query
      |> from(aggregations: [event_count: count()])

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)
    assert decoded["aggregations"] == [%{"name" => "event_count", "type" => "count"}]
    # IO.puts json
  end

  test "build a query with multiple fields in aggregation" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day
      )

    query =
      query
      |> from(aggregations: [my_cardinality: cardinality(["f1", "f2"], byRow: true)])

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["aggregations"] == [
             %{
               "name" => "my_cardinality",
               "type" => "cardinality",
               "fields" => ["f1", "f2"],
               "byRow" => true
             }
           ]

  end

  test "build query with column comparison filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == dimensions["bar"]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)
    assert decoded["filter"] == %{"type" => "columnComparison", "dimensions" => ["foo", "bar"]}
  end

  test "build query with selector filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == "bar"
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)
    assert decoded["filter"] == %{"type" => "selector", "dimension" => "foo", "value" => "bar"}
  end

  test "build query with two filters ANDed together" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == "bar" and dimensions.bar == dimensions.foo
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "and",
             "fields" => [
               %{"type" => "selector", "dimension" => "foo", "value" => "bar"},
               %{"type" => "columnComparison", "dimensions" => ["bar", "foo"]}
             ]
           }
  end

  test "build query with three filters ANDed together" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter:
          dimensions.foo == "bar" and dimensions.bar == dimensions.foo and 0 < dimensions.baz < 10
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "and",
             "fields" => [
               %{"type" => "selector", "dimension" => "foo", "value" => "bar"},
               %{"type" => "columnComparison", "dimensions" => ["bar", "foo"]},
               %{
                 "type" => "bound",
                 "dimension" => "baz",
                 "ordering" => "numeric",
                 "lower" => "0",
                 "lowerStrict" => true,
                 "upper" => "10",
                 "upperStrict" => true
               }
             ]
           }
  end

  test "build query with two filters ORed together" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == "bar" or dimensions.bar == dimensions.foo
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "or",
             "fields" => [
               %{"type" => "selector", "dimension" => "foo", "value" => "bar"},
               %{"type" => "columnComparison", "dimensions" => ["bar", "foo"]}
             ]
           }
  end

  test "build query with AND and OR filters" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        # 'and' has higher precedence, so this should get parsed as
        # (foo == "bar" or (bar == foo and baz == 17))
        filter:
          dimensions.foo == "bar" or (dimensions.bar == dimensions.foo and dimensions.baz == 17)
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "or",
             "fields" => [
               %{"type" => "selector", "dimension" => "foo", "value" => "bar"},
               %{
                 "type" => "and",
                 "fields" => [
                   %{"type" => "columnComparison", "dimensions" => ["bar", "foo"]},
                   %{"type" => "selector", "dimension" => "baz", "value" => 17}
                 ]
               }
             ]
           }
  end

  test "build query with a not equal to filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo != "bar"
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "not",
             "field" => %{"type" => "selector", "dimension" => "foo", "value" => "bar"}
           }
  end

  test "build query with a not equal to filter based on columnComparison" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo != dimensions.bar
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "not",
             "field" => %{"type" => "columnComparison", "dimensions" => ["foo", "bar"]}
           }
  end

  test "equal filter can't have value on the left hand side" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
      )

    assert_raise RuntimeError, "left operand of == must be a dimension", fn ->
      ast =
        quote do
          use Panoramix

          from(unquote(query),
            filter: "bar" == dimensions.foo
          )
        end

      Code.eval_quoted(ast)
    end

    assert_raise RuntimeError, "left operand of != must be a dimension", fn ->
      ast =
        quote do
          use Panoramix

          from(unquote(query),
            filter: "bar" != dimensions.foo
          )
        end

      Code.eval_quoted(ast)
    end
  end

  test "build query with a NOT filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: not (dimensions.foo == "bar")
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "not",
             "field" => %{"type" => "selector", "dimension" => "foo", "value" => "bar"}
           }
  end

  test "build query with an 'in' filter" do
    x = "baz"

    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo in ["bar", x]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "in",
             "dimension" => "foo",
             "values" => ["bar", "baz"]
           }
  end

  test "build query with a non-strict 'bound' filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: 1 <= dimensions.foo <= 10
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "bound",
             "dimension" => "foo",
             "lower" => "1",
             "upper" => "10",
             "lowerStrict" => false,
             "upperStrict" => false,
             "ordering" => "numeric"
           }
  end

  test "build query with a lower-strict 'bound' filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: "aaa" < dimensions.foo <= "bbb"
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "bound",
             "dimension" => "foo",
             "lower" => "aaa",
             "upper" => "bbb",
             "lowerStrict" => true,
             "upperStrict" => false,
             "ordering" => "lexicographic"
           }
  end

  test "build query with an 'expression' filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: expression("foo / 1000 < bar")
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)
    assert decoded["filter"] == %{"type" => "expression", "expression" => "foo / 1000 < bar"}
  end

  test "add extra filter to existing query" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == "bar"
      )

    query =
      from(query,
        filter: dimensions.bar == "baz" and ^query.filter
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert decoded["filter"] == %{
             "type" => "and",
             "fields" => [
               %{"type" => "selector", "dimension" => "bar", "value" => "baz"},
               %{"type" => "selector", "dimension" => "foo", "value" => "bar"}
             ]
           }
  end

  test "add extra filter to a 'nil' filter with 'and'" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
      )

    # Adding a new filter here, when there is no existing filter,
    # means that the new filter just gets used as the query filter.
    query =
      from(query,
        filter: dimensions.bar == "baz" and ^query.filter
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)
    assert decoded["filter"] == %{"type" => "selector", "dimension" => "bar", "value" => "baz"}
  end

  test "cannot add filter to 'nil' filter with 'or'" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
      )

    # It's not meaningful to use the empty filter in an "or" expression
    assert_raise RuntimeError, "right operand to 'or' must not be nil", fn ->
      from(query,
        filter: dimensions.bar == "baz" or ^query.filter
      )
    end

    assert_raise RuntimeError, "left operand to 'or' must not be nil", fn ->
      from(query,
        filter: ^query.filter or dimensions.bar == "baz"
      )
    end
  end

  test "cannot apply 'not' to 'nil' filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"]
      )

    # It's not meaningful to use the empty filter in an "or" expression
    assert_raise RuntimeError, "operand to 'not' must not be nil", fn ->
      from(query,
        filter: not (^query.filter)
      )
    end
  end

  test "extract filter from JSON object" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == "bar"
      )

    json = Panoramix.Query.to_json(query)
    %{"filter" => filter} = Jason.decode!(json)

    new_query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: ^filter
      )

    assert %{"filter" => %{"type" => "selector", "dimension" => "foo", "value" => "bar"}} =
             Jason.decode!(Panoramix.Query.to_json(new_query))
  end

  test "build a topN query" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        dimension: "foo",
        metric: "size",
        threshold: 10
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{"queryType" => "topN", "dimension" => "foo", "metric" => "size", "threshold" => 10} =
             decoded
  end

  test "build a query with a query context" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        filter: dimensions.foo == "bar",
        context: %{
          timeout: 0,
          priority: 100,
          queryId: "my-unique-query-id",
          skipEmptyBuckets: true
        }
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "timeout" => 0,
             "priority" => 100,
             "queryId" => "my-unique-query-id",
             "skipEmptyBuckets" => true
           } == decoded["context"]
  end

  test "build a query with a query context while supplying default values from app config" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        context: %{queryId: "my-unique-query-id", skipEmptyBuckets: true}
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "timeout" => 120_000,
             "priority" => 0,
             "queryId" => "my-unique-query-id",
             "skipEmptyBuckets" => true
           } == decoded["context"]
  end

  test "add query context to an existing query and maintain defaults from app config" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        context: %{queryId: "my-unique-query-id", skipEmptyBuckets: true}
      )

    query =
      from(query,
        context: %{queryId: "another-unique-query-id", skipEmptyBuckets: false}
      )

    json = Panoramix.Query.to_json(query)
    decoded = Jason.decode!(json)

    assert %{
             "timeout" => 120_000,
             "priority" => 0,
             "queryId" => "another-unique-query-id",
             "skipEmptyBuckets" => false
           } == decoded["context"]
  end

  test "build a query with an arithmetic post-aggregation" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [event_count: count(), unique_ids: hyperUnique(:user_unique)],
        post_aggregations: [
          mean_events_per_user: aggregations.event_count / aggregations["unique_ids"]
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert [
             %{
               "type" => "arithmetic",
               "name" => "mean_events_per_user",
               "fn" => "/",
               "fields" => [
                 %{"type" => "fieldAccess", "fieldName" => "event_count"},
                 %{"type" => "fieldAccess", "fieldName" => "unique_ids"}
               ]
             }
           ] == decoded["postAggregations"]
  end

  test "build a query with an arithmetic post-aggregation including constant" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [event_count: count(), unique_ids: hyperUnique(:user_unique)],
        post_aggregations: [
          mean_events_per_user_pct: aggregations.event_count / aggregations["unique_ids"] * 100
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert [
             %{
               "type" => "arithmetic",
               "name" => "mean_events_per_user_pct",
               "fn" => "*",
               "fields" => [
                 %{
                   "type" => "arithmetic",
                   "fn" => "/",
                   "fields" => [
                     %{"type" => "fieldAccess", "fieldName" => "event_count"},
                     %{"type" => "fieldAccess", "fieldName" => "unique_ids"}
                   ]
                 },
                 %{"type" => "constant", "value" => 100}
               ]
             }
           ] == decoded["postAggregations"]
  end

  test "build a query with post-aggregation functions" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        aggregations: [event_count: count(), unique_ids: hyperUnique(:user_unique)],
        post_aggregations: [
          cardinality: hyperUniqueCardinality(:unique_ids),
          greatest: doubleGreatest([aggregations.event_count, aggregations.unique_ids]),
          histogram: buckets(:histogram_data, bucketSize: 42, offset: 17)
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert [
             %{
               "type" => "hyperUniqueCardinality",
               "name" => "cardinality",
               "fieldName" => "unique_ids"
             },
             %{
               "type" => "doubleGreatest",
               "name" => "greatest",
               "fields" => [
                 %{"fieldName" => "event_count", "type" => "fieldAccess"},
                 %{"fieldName" => "unique_ids", "type" => "fieldAccess"}
               ]
             },
             %{
               "name" => "histogram",
               "type" => "buckets",
               "fieldName" => "histogram_data",
               "bucketSize" => 42,
               "offset" => 17
             }
           ] == decoded["postAggregations"]
  end

  test "build a segmentMetadata query" do
    query =
      from("my_datasource",
        query_type: "segmentMetadata",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        to_include: :all,
        merge: true,
        analysis_types: [:cardinality, :minmax]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "segmentMetadata",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
             "dataSource" => "my_datasource",
             "toInclude" => %{"type" => "all"},
             "merge" => true,
             "analysisTypes" => ["cardinality", "minmax"],
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a segmentMetadata query limited to certain columns" do
    query =
      from("my_datasource",
        query_type: "segmentMetadata",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        to_include: ["foo", "bar"],
        merge: true,
        analysis_types: [:cardinality, :minmax]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "segmentMetadata",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
             "dataSource" => "my_datasource",
             "toInclude" => %{"type" => "list", "columns" => ["foo", "bar"]},
             "merge" => true,
             "analysisTypes" => ["cardinality", "minmax"],
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a query using date structs" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: [{~D[2018-05-29], ~D[2018-06-05]}],
        granularity: :day
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29/2018-06-05"],
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a query using datetime structs" do
    from = Timex.to_datetime({{2018, 5, 29}, {1, 30, 0}})
    to = Timex.to_datetime({{2018, 6, 5}, {18, 0, 0}})

    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: [{from, to}],
        granularity: :day
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "intervals" => ["2018-05-29T01:30:00+00:00/2018-06-05T18:00:00+00:00"],
             "granularity" => "day",
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a timeBoundary query with a 'maxTime' bound" do
    query =
      from("my_datasource",
        query_type: "timeBoundary",
        bound: :maxTime
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeBoundary",
             "dataSource" => "my_datasource",
             "bound" => "maxTime",
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a query with a virtual column" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        virtual_columns: [plus_one: expression("foo + 1", :long)],
        aggregations: [plus_one_sum: longSum(:plus_one)]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
             "virtualColumns" => [
               %{
                 "name" => "plus_one",
                 "type" => "expression",
                 "expression" => "foo + 1",
                 "outputType" => "long"
               }
             ],
             "aggregations" => [
               %{"name" => "plus_one_sum", "type" => "longSum", "fieldName" => "plus_one"}
             ],
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a query with an interval filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
        granularity: :day,
        # Let's use all three kinds of intervals we support: strings, dates and datetimes
        filter:
          dimensions.__time in intervals([
            "2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00",
            {~D[2018-06-05], ~D[2018-06-12]},
            {Timex.to_datetime({{2018, 6, 12}, {1, 30, 0}}),
             Timex.to_datetime({{2018, 6, 19}, {18, 0, 0}})}
          ])
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
             "filter" => %{
               "type" => "interval",
               "dimension" => "__time",
               "intervals" => [
                 "2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00",
                 "2018-06-05/2018-06-12",
                 "2018-06-12T01:30:00+00:00/2018-06-19T18:00:00+00:00"
               ]
             },
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "build a query filtering on a lookup" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
        granularity: :day,
        filter: dimensions.foo |> lookup(:foo_to_bar) == "expected_bar"
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
             "filter" => %{
               "type" => "selector",
               "dimension" => "foo",
               "value" => "expected_bar",
               "extractionFn" => %{"type" => "registeredLookup", "lookup" => "foo_to_bar"}
             },
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "use lookup in column comparison filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
        granularity: :day,
        filter:
          dimensions["foo"] |> lookup(:foo_to_bar, replaceMissingValueWith: "missing") ==
            dimensions.baz
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
             "filter" => %{
               "type" => "columnComparison",
               "dimensions" => [
                 %{
                   "dimension" => "foo",
                   "extractionFn" => %{
                     "lookup" => "foo_to_bar",
                     "type" => "registeredLookup",
                     "replaceMissingValueWith" => "missing"
                   },
                   "type" => "extraction"
                 },
                 "baz"
               ]
             },
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "chain two lookups in filter" do
    query =
      from("my_datasource",
        query_type: "timeseries",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
        granularity: :day,
        filter:
          dimensions.foo
          |> lookup(:foo_to_bar)
          |> lookup(:bar_to_baz, retainMissingValue: true, injective: true) == "expected_baz"
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => "my_datasource",
             "granularity" => "day",
             "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-20T00:00:00+00:00"],
             "filter" => %{
               "type" => "selector",
               "dimension" => "foo",
               "value" => "expected_baz",
               "extractionFn" => %{
                 "type" => "cascade",
                 "extractionFns" => [
                   %{"type" => "registeredLookup", "lookup" => "foo_to_bar"},
                   %{
                     "type" => "registeredLookup",
                     "lookup" => "bar_to_baz",
                     "retainMissingValue" => true,
                     "injective" => true
                   }
                 ]
               }
             },
             "context" => %{"timeout" => 120_000, "priority" => 0}
           } == decoded
  end

  test "builds a query with hllSketchEstimate post-aggregation" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        metric: %{type: "dimension"},
        threshold: 10,
        dimension: "foo",
        aggregations: [
          hyper_unique_agg: hyperUnique(:hyper_unique, round: true),
          hll_sketch_agg: hllSketchMerge(:hll_sketch, round: true)
        ],
        post_aggregations: [
          post_agg: aggregations.hyper_unique_agg / hllSketchEstimate(aggregations.hll_sketch_agg)
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)

    assert Jason.decode!(json) ==
             %{
               "aggregations" => [
                 %{
                   "fieldName" => "hyper_unique",
                   "name" => "hyper_unique_agg",
                   "round" => true,
                   "type" => "hyperUnique"
                 },
                 %{
                   "fieldName" => "hll_sketch",
                   "name" => "hll_sketch_agg",
                   "round" => true,
                   "type" => "HLLSketchMerge"
                 }
               ],
               "context" => %{"priority" => 0, "timeout" => 120_000},
               "dataSource" => "my_datasource",
               "dimension" => "foo",
               "granularity" => "day",
               "intervals" => ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
               "metric" => %{"type" => "dimension"},
               "postAggregations" => [
                 %{
                   "fields" => [
                     %{"fieldName" => "hyper_unique_agg", "type" => "fieldAccess"},
                     %{
                       "field" => %{"fieldName" => "hll_sketch_agg", "type" => "fieldAccess"},
                       "type" => "HLLSketchEstimate"
                     }
                   ],
                   "fn" => "/",
                   "name" => "post_agg",
                   "type" => "arithmetic"
                 }
               ],
               "queryType" => "topN",
               "threshold" => 10
             }
  end

  test "builds a query with hllSketchEstimate with options" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        metric: %{type: "dimension"},
        threshold: 10,
        dimension: "foo",
        aggregations: [
          hyper_unique_agg: hyperUnique(:hyper_unique, round: true),
          hll_sketch_agg: hllSketchMerge(:hll_sketch, round: true)
        ],
        post_aggregations: [
          post_agg:
            aggregations.hyper_unique_agg /
              hllSketchEstimate(aggregations.hll_sketch_agg, round: true)
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)

    assert Jason.decode!(json)["postAggregations"] == [
             %{
               "fields" => [
                 %{"fieldName" => "hyper_unique_agg", "type" => "fieldAccess"},
                 %{
                   "field" => %{"fieldName" => "hll_sketch_agg", "type" => "fieldAccess"},
                   "round" => true,
                   "type" => "HLLSketchEstimate"
                 }
               ],
               "fn" => "/",
               "name" => "post_agg",
               "type" => "arithmetic"
             }
           ]
  end

  test "builds a query with hllSketchUnion" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        metric: %{type: "dimension"},
        threshold: 10,
        dimension: "foo",
        aggregations: [
          sketch_a: hllSketchMerge(:hll_sketch_a, round: true),
          sketch_b: hllSketchMerge(:hll_sketch_b, round: true)
        ],
        post_aggregations: [
          post_agg: hllSketchUnion([aggregations.sketch_a, aggregations.sketch_b])
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)

    assert Jason.decode!(json)["postAggregations"] == [
             %{
               "fields" => [
                 %{"fieldName" => "sketch_a", "type" => "fieldAccess"},
                 %{"fieldName" => "sketch_b", "type" => "fieldAccess"}
               ],
               "name" => "post_agg",
               "type" => "HLLSketchUnion"
             }
           ]
  end

  test "builds a query with hllSketchUnion with options" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        metric: %{type: "dimension"},
        threshold: 10,
        dimension: "foo",
        aggregations: [
          sketch_a: hllSketchMerge(:hll_sketch_a, round: true),
          sketch_b: hllSketchMerge(:hll_sketch_b, round: true)
        ],
        post_aggregations: [
          post_agg:
            hllSketchEstimate(
              hllSketchUnion(
                [aggregations.sketch_a, aggregations.sketch_b],
                lgK: 2,
                tgtHllType: "HLL_4"
              )
            )
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)

    assert Jason.decode!(json)["postAggregations"] == [
             %{
               "field" => %{
                 "fields" => [
                   %{"fieldName" => "sketch_a", "type" => "fieldAccess"},
                   %{"fieldName" => "sketch_b", "type" => "fieldAccess"}
                 ],
                 "lgK" => 2,
                 "tgtHllType" => "HLL_4",
                 "type" => "HLLSketchUnion"
               },
               "name" => "post_agg",
               "type" => "HLLSketchEstimate"
             }
           ]
  end

  test "builds a query with theta sketch operations" do
    query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
        granularity: :day,
        metric: %{type: "dimension"},
        threshold: 10,
        dimension: "foo",
        aggregations: [
          sketch_a: thetaSketch(:foo),
          sketch_b: thetaSketch(:bar)
        ],
        post_aggregations: [
          constant: thetaSketchConstant("AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI="),
          post_a: thetaSketchEstimate(aggregations.sketch_a, errorBoundsStdDev: 1),
          post_b: thetaSketchEstimate(thetaSketchSetOp(:intersect, [aggregations.sketch_a, aggregations.sketch_b, aggregations.constant])),
          a_to_string: thetaSketchToString(aggregations.sketch_a)
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)

    assert Jason.decode!(json)["postAggregations"] == [
      %{"name" => "constant",
        "type" => "thetaSketchConstant",
        "value" => "AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI="},
      %{"name" => "post_a",
        "type" => "thetaSketchEstimate",
        "field" => %{"type" => "fieldAccess", "fieldName" => "sketch_a"},
        "errorBoundsStdDev" => 1},
      %{"name" => "post_b",
        "type" => "thetaSketchEstimate",
        "field" => %{"type" => "thetaSketchSetOp",
                     "func" => "INTERSECT",
                     "fields" => [%{"type" => "fieldAccess", "fieldName" => "sketch_a"},
                                  %{"type" => "fieldAccess", "fieldName" => "sketch_b"},
                                  %{"type" => "fieldAccess", "fieldName" => "constant"}]}},
      %{"name" => "a_to_string",
        "type" => "thetaSketchToString",
        "field" => %{"type" => "fieldAccess", "fieldName" => "sketch_a"}}
    ]
  end

  test "nested query" do
    inner_query =
      from("my_datasource",
        query_type: "topN",
        intervals: ["2020-11-01/P7D"],
        granularity: :day,
        aggregations: [event_count: count()],
        dimension: "foo",
        metric: "event_count",
        threshold: 10
      )

    query =
      from(%{type: :query, query: inner_query},
        query_type: "timeseries",
        intervals: ["2020-11-01/P7D"],
        granularity: :day,
        aggregations: [foo_count: count(), event_count: longSum(:event_count)],
        post_aggregations: [
          mean_events_per_foo: aggregations.event_count / aggregations.foo_count
        ]
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "timeseries",
             "dataSource" => %{
               "type" => "query",
               "query" => %{
                 "aggregations" => [%{"name" => "event_count", "type" => "count"}],
                 "context" => %{"priority" => 0, "timeout" => 120_000},
                 "dataSource" => "my_datasource",
                 "dimension" => "foo",
                 "granularity" => "day",
                 "intervals" => ["2020-11-01/P7D"],
                 "metric" => "event_count",
                 "queryType" => "topN",
                 "threshold" => 10
               }
             },
             "context" => %{"priority" => 0, "timeout" => 120_000},
             "granularity" => "day",
             "intervals" => ["2020-11-01/P7D"],
             "aggregations" => [
               %{"name" => "foo_count", "type" => "count"},
               %{"fieldName" => "event_count", "name" => "event_count", "type" => "longSum"}
             ],
             "postAggregations" => [
               %{
                 "name" => "mean_events_per_foo",
                 "fn" => "/",
                 "type" => "arithmetic",
                 "fields" => [
                   %{"fieldName" => "event_count", "type" => "fieldAccess"},
                   %{"fieldName" => "foo_count", "type" => "fieldAccess"}
                 ]
               }
             ]
           } ==
             decoded
  end

  test "join query" do
    query =
      from(
        %{
          type: :join,
          left: "my_datasource",
          right: %{type: :lookup, lookup: "my_lookup"},
          rightPrefix: "r.",
          condition: "foo = \"r.k\"",
          joinType: :inner
        },
        query_type: "topN",
        intervals: ["2020-11-01/P7D"],
        granularity: :day,
        aggregations: [foo_count: count()],
        dimension: "r.v",
        metric: :foo_count,
        threshold: 10
      )

    json = Panoramix.Query.to_json(query)
    assert is_binary(json)
    decoded = Jason.decode!(json)

    assert %{
             "queryType" => "topN",
             "aggregations" => [%{"name" => "foo_count", "type" => "count"}],
             "context" => %{"priority" => 0, "timeout" => 120_000},
             "dataSource" => %{
               "type" => "join",
               "joinType" => "inner",
               "left" => "my_datasource",
               "right" => %{"lookup" => "my_lookup", "type" => "lookup"},
               "rightPrefix" => "r.",
               "condition" => "foo = \"r.k\""
             },
             "dimension" => "r.v",
             "granularity" => "day",
             "intervals" => ["2020-11-01/P7D"],
             "metric" => "foo_count",
             "threshold" => 10
           } == decoded
  end

  test "subtotals spec" do
    query =
      from "table",
        query_type: "groupBy",
        subtotals_spec: [[:d1], [:d2, :d3]]

    assert query.subtotals_spec

    json = Panoramix.Query.to_json(query)

    assert %{
             "queryType" => "groupBy",
             "context" => %{"priority" => 0, "timeout" => 120_000},
             "subtotalsSpec" => [["d1"], ["d2", "d3"]]
           } = Jason.decode!(json)
  end

  test "dynamic building of aggregations" do
    aggregations = [
      %{
        type: "filtered",
        filter: %{
          type: "selector",
          dimension: "dimension_id",
          value: "value"
        },
        aggregator: %{
          type: "longSum",
          fieldName: "__count",
          name: "dynamic_aggregator"
        }
      }
    ]

    query =
      from "table",
        query_type: "topN",
        aggregations: ^aggregations,
        dimension: :sum

    assert ^aggregations = query.aggregations
  end

  test "extend query with new aggregations, post-aggregations, virtual columns" do
    query1 = from "table",
      query_type: "timeseries",
      virtual_columns: [
        plus_one: expression("foo + 1", :long)
      ],
      aggregations: [
        foo_sum: longSum(:foo),
        event_count: count()
      ]

    assert %{"aggregations" => [%{"fieldName" => "foo", "name" => "foo_sum", "type" => "longSum"},
                                %{"name" => "event_count", "type" => "count"}],
             "queryType" => "timeseries",
             "virtualColumns" => [%{"expression" => "foo + 1", "name" => "plus_one", "outputType" => "long", "type" => "expression"}]} =
      query1 |> Panoramix.Query.to_json() |> Jason.decode!()

    query2 = from query1,
      query_type: "topN",
      virtual_columns: [
        plus_two: expression("foo + 2", :double)
      ],
      aggregations: [
        event_count: longSum(:count),
        plus_one_sum: longSum(:plus_one)
      ],
      post_aggregations: [
        sum_all: aggregations.foo_sum + aggregations.plus_one_sum
      ]

    # The "event_count" aggregation has been changed,
    # the "foo_sum" aggregation stays the same,
    # and the "plus_one_sum" aggregation has been added.
    assert %{"aggregations" => [%{"fieldName" => "foo", "name" => "foo_sum", "type" => "longSum"},
                                %{"fieldName" => "count", "name" => "event_count", "type" => "longSum"},
                                %{"fieldName" => "plus_one", "name" => "plus_one_sum", "type" => "longSum"}],
             # A post-aggregation has been added
             "postAggregations" => [%{"fields" => [%{"fieldName" => "foo_sum", "type" => "fieldAccess"}, %{"fieldName" => "plus_one_sum", "type" => "fieldAccess"}], "fn" => "+", "name" => "sum_all", "type" => "arithmetic"}],
             # The query type has changed
             "queryType" => "topN",
             # A virtual column has been added
             "virtualColumns" => [%{"expression" => "foo + 1", "name" => "plus_one", "outputType" => "long", "type" => "expression"},
                                  %{"expression" => "foo + 2", "name" => "plus_two", "outputType" => "double", "type" => "expression"}]} =
      query2 |> Panoramix.Query.to_json() |> Jason.decode!()
  end
end
