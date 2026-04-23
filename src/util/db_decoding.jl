"""
Decoding utilities for items and parameter values stored in Spine database.
"""

using JSON
using DataFrames
using Dates

# Deduplicate dimension class names using 1-indexed suffixes for repeated names.
# E.g. ["unit", "node", "node"] → ["unit", "node_1", "node_2"]
function _dedup_dim_col_names(dim_classes)
    counts = Dict{String,Int}()
    for d in dim_classes; counts[string(d)] = get(counts, string(d), 0) + 1; end
    seen = Dict{String,Int}()
    result = String[]
    for d in dim_classes
        s = string(d)
        if counts[s] == 1
            push!(result, s)
        else
            seen[s] = get(seen, s, 0) + 1
            push!(result, "$(s)_$(seen[s])")
        end
    end
    return result
end

# Replace all missing values in a DataFrame with nothing (NULL → nothing, not missing)
function _nulls_to_nothing!(df::DataFrame)
    for col in names(df)
        if any(ismissing, df[!, col])
            df[!, col] = [ismissing(v) ? nothing : v for v in df[!, col]]
        end
    end
    return df
end

# Enrich a type tag: when type_str == "list_value_ref", look up the actual element type
# in list_value and return "list_value_ref{actual_type}". Falls back to type_str on any error.
function _enrich_type(type_str, blob, db)
    if !ismissing(type_str) && !isnothing(type_str) && type_str == "list_value_ref" &&
       !ismissing(blob) && !isnothing(blob)
        blob_str = blob isa Vector{UInt8} ? String(copy(blob)) : string(blob)
        try
            list_id = parse(Int, strip(blob_str))
            lv_result = DBInterface.execute(
                db, "SELECT type FROM list_value WHERE id = ?", [list_id]
            ) |> DataFrame
            if nrow(lv_result) > 0
                lv_type = lv_result[1, :type]
                return (ismissing(lv_type) || isnothing(lv_type)) ?
                    type_str : "list_value_ref{$(lv_type)}"
            end
        catch
        end
    end
    return type_str
end

"""
    _decode_parameter_value(type_str, blob_value; db=nothing, detailed=false) -> Any

Internal helper — not intended for direct use; call `get_parameter_value` instead.

Decode a SpineOpt parameter value from its raw database representation (BLOB bytes +
type tag string) to the corresponding Julia value.

# Background
SpineOpt stores every parameter value as a pair of columns in the SQLite database:
- `type`  — a string tag identifying the value kind (e.g. `"float"`, `"time_series"`)
- `value` — a BLOB that encodes the actual data, whose interpretation depends on `type`

This function is the single place that translates that (type, blob) pair into a usable
Julia value, and is called by `get_parameter_definitions`, `get_parameter_values`, and
`get_parameter_value` in `SpineDataFrame`.

# Arguments
`type_str` and `blob_value` are always passed as a matched pair read directly from one of
two SpineOpt database tables — never constructed by the caller:

| Source table          | `type_str` column  | `blob_value` column |
|:----------------------|:-------------------|:--------------------|
| `parameter_definition` (default values) | `:default_type` | `:default_value` |
| `parameter_value`     (entity values)   | `:value_type`   | `:value`         |

Example (how the callers in `SpineDataFrame` obtain the pair):
```julia
row = DBInterface.execute(db, "SELECT type, value FROM parameter_value WHERE ...") |> DataFrame
_decode_parameter_value(row[1, :type], row[1, :value]; db=db, detailed=true)
```

- `type_str`: the `type` column — a string tag identifying the value kind
  (e.g. `"float"`, `"time_series"`).
- `blob_value`: the `value` column — a text encoding whose format depends on `type_str`:
  plain text for scalars (e.g. `"3.14"`, `"true"`), a JSON object `{"data": ...}` for
  compound types, and a plain integer ID string for `list_value_ref`. SQLite may deliver
  it as `Vector{UInt8}` bytes or already as a `String`; both are handled transparently.

# Keyword arguments
- `db`: open `SQLite.DB` connection. Required only for `list_value_ref` resolution
  (a lookup into the `list_value` table). Pass `nothing` to skip resolution and return
  the raw integer ID string as a fallback.
- `detailed`: controls how compound types are decoded.
  - `false` (default) — compound types return a placeholder string, e.g. `"<time_series>"`.
    Use this for overview queries where full decoding of every row would be expensive.
  - `true` — compound types are fully parsed into rich Julia types (see table below).
    Use this when inspecting a single parameter value in detail.

# Return values by type tag

| `type_str`       | `detailed=false`    | `detailed=true`                                      |
|:-----------------|:--------------------|:-----------------------------------------------------|
| `"float"`        | `Float64`           | `Float64`                                            |
| `"bool"`         | `Bool`              | `Bool`                                               |
| `"str"`          | `String`            | `String`                                             |
| `"list_value_ref"` | `String` (raw id or resolved value) | same                          |
| `"duration"`     | `"<duration>"`      | `String` (e.g. `"1h"`)                               |
| `"time_series"`  | `"<time_series>"`   | `DataFrame` with columns `timestamp::String`, `value` (sorted by timestamp) |
| `"array"`        | `"<array>"`         | `Vector{Any}`                                        |
| `"map"`          | `"<map>"`           | `Dict`                                               |
| anything else    | `"<type_str>"`      | raw blob string                                      |

Returns `nothing` if either `type_str` or `blob_value` is `missing` or `nothing`.
On parse errors, scalar types fall back to the raw blob string; compound types emit
a `@warn` (for `time_series`) or silently fall back to the raw blob string.
"""
function _decode_parameter_value(type_str, blob_value; db=nothing, detailed=false)
    if ismissing(type_str) || isnothing(type_str) || ismissing(blob_value) || isnothing(blob_value)
        return nothing
    end

    # Convert BLOB (may arrive as Vector{UInt8}) to String
    blob_str = blob_value isa Vector{UInt8} ? String(copy(blob_value)) : string(blob_value)

    if type_str == "float"
        try return parse(Float64, blob_str) catch; return blob_str end
    elseif type_str == "bool"
        return blob_str == "1" || lowercase(blob_str) == "true"
    elseif type_str == "str"
        return blob_str
    elseif type_str == "list_value_ref"
        if !isnothing(db)
            try
                list_value_id = parse(Int, strip(blob_str))
                result = DBInterface.execute(
                    db, "SELECT value FROM list_value WHERE id = ?", [list_value_id]
                ) |> DataFrame
                if nrow(result) > 0
                    val = result[1, :value]
                    return val isa Vector{UInt8} ? String(copy(val)) : string(val)
                end
            catch e
                @warn "Failed to resolve list_value_ref id=$blob_str: $e"
            end
        end
        return blob_str  # fallback: return raw id string
    elseif detailed
        if type_str == "duration"
            try
                parsed = JSON.parse(blob_str)
                return string(get(parsed, "data", blob_str))
            catch
                return blob_str
            end
        elseif type_str == "time_series"
            try
                parsed = JSON.parse(blob_str)
                
                # the `parsed`` is of form `JSON.Object{String, Any}` with "index", "data" and "type" entries
                # `Time series fixed resolution` in Spine database
                if parsed isa AbstractDict && haskey(parsed, "index") && parsed["data"] isa AbstractArray
                    idx = parsed["index"]
                    start_s = get(idx, "start", "0000-01-01 00:00:00")
                    res_s = get(idx, "resolution", "1h")
                    n = length(parsed["data"])
                    
                    # parse start time into valid datetime format for timestamp construction: "yyyy-mm-ddTHH:MM:SS"
                    start_dt = DateTime(start_s, dateformat"yyyy-mm-dd HH:MM:SS")  

                    period = try _parse_resolution(string(res_s)) catch; Hour(1) end
                    vals = collect(parsed["data"])
                    if !isempty(vals)
                        timestamps = [start_dt + period*(i-1) for i in 1:length(vals)]
                        ts_clean = [Dates.format(t, dateformat"yyyy-mm-ddTHH:MM:SS") for t in timestamps]
                        return DataFrame(timestamp=ts_clean, value=vals)
                    else
                        @warn "empty time_series data"
                        return blob_str
                    end
            
                # blob_str = "{\"data\": {\"2025-01-01T00:00:00\": 0.46}}"
                # parsed = JSON.parse(blob_str)
                # `Time series variable resolution` in Spine database 
                else 
                    data = get(parsed, "data", Dict())
                    data isa AbstractDict || (
                        @warn "Expected time_series data to be parsed by JSON as an AbstractDict, got $(typeof(data))";
                        return blob_str
                    )
                    timestamps = sort(collect(keys(data)))
                    vals = [data[ts] for ts in timestamps]
                    ts_clean = [replace(ts, r"\\.0$" => "") for ts in timestamps]
                    return DataFrame(timestamp=ts_clean, value=vals)
                end
            catch e
                @warn "Failed to parse time_series: $e"
            end
            return blob_str
        elseif type_str == "array"
            try
                parsed = JSON.parse(blob_str)
                return get(parsed, "data", Any[])
            catch
                return blob_str
            end
        elseif type_str == "map"
            try
                parsed = JSON.parse(blob_str)
                return get(parsed, "data", Dict())
            catch
                return blob_str
            end
        else
            return blob_str
        end
    else
        # Non-detailed: compound types → placeholder string
        return "<$(type_str)>"
    end
end


# parse resolution like "1h", "15m", "1D"
function _parse_resolution(res::AbstractString)
    m = match(r"^(\\d+)\s*([smhdw])$", lowercase(strip(res)))
    if m === nothing
        return Hour(1)
    end
    val = parse(Int, m.captures[1])
    unit = m.captures[2]
    if unit == "s"
        return Second(val)
    elseif unit == "m"
        return Minute(val)
    elseif unit == "h"
        return Hour(val)
    elseif unit == "D"
        return Day(val)
    else
        return Hour(1)
    end
end
