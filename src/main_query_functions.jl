"""
    get_alternatives(db_path::AbstractString) -> DataFrame

List all alternatives of a Spine database. Close the database connection after use.

# Arguments
- `db_path`: Path to the SQLite database file

# Returns
A DataFrame with columns: alternative (name), description.

# Example
```julia
df_alts = get_alternatives("SpineDB.sqlite")
```
"""
function get_alternatives(db::SQLite.DB)::DataFrame
    query = """
        SELECT name AS alternative, description
        FROM alternative
        ORDER BY name
    """
    return DBInterface.execute(db, query) |> DataFrame |> _nulls_to_nothing!
end
get_alternatives(db_path::AbstractString)::DataFrame = connect_db(db_path) do db
    return get_alternatives(db)
end

"""
    get_entity_classes(db_path::AbstractString) -> DataFrame

List all entity classes in the database. Close the database connection after use.

# Arguments
- `db_path`: Path to the SQLite database file

# Returns
A DataFrame with columns: entity_class (name).

# Example
```julia
df_entity_classes = get_entity_classes("SpineDB.sqlite")
```
"""
function get_entity_classes(db::SQLite.DB)::DataFrame
    query = """
        SELECT name as entity_class
        FROM entity_class
        ORDER BY name
    """
    return DBInterface.execute(db, query) |> DataFrame
end
get_entity_classes(db_path::AbstractString)::DataFrame = connect_db(db_path) do db
    return get_entity_classes(db)
end

"""
    get_entity_names(db, entity_class) -> DataFrame

List all entity names for a given entity class.

For simple object classes, returns a single-column DataFrame with the entity class name as
the column header.

For relationship classes (with dimensions), returns a wide DataFrame:
- Column 1: full entity name (column header = entity_class name)
- Columns 2…N+1: member entity names per dimension (column headers = dimension class names)

# Example
```julia
get_entity_names("SpineDB.sqlite", "unit")
# → DataFrame with column: unit

get_entity_names("SpineDB.sqlite", "unit__from_node")
# → DataFrame with columns: unit__from_node, unit, node
```
"""
function get_entity_names(db::SQLite.DB, entity_class::AbstractString)
    # Resolve class id
    class_result = DBInterface.execute(
        db, "SELECT id FROM entity_class WHERE name = ?", [entity_class]
    ) |> DataFrame
    if nrow(class_result) == 0
        error("Entity class '$entity_class' not found in database")
    end
    class_id = class_result[1, :id]

    # Check whether this is a relationship class (has dimension entries)
    dim_result = DBInterface.execute(db, """
        SELECT ec.name AS dim_class, ecd.position
        FROM entity_class_dimension ecd
        JOIN entity_class ec ON ecd.dimension_id = ec.id
        WHERE ecd.entity_class_id = ?
        ORDER BY ecd.position
    """, [class_id]) |> DataFrame

    if nrow(dim_result) == 0
        # Simple object class — single column
        return DBInterface.execute(db, """
            SELECT name AS '$(entity_class)'
            FROM entity
            WHERE class_id = ?
            ORDER BY name
        """, [class_id]) |> DataFrame
    end

    # Relationship class — fetch entities and their member names per dimension
    entities_df = DBInterface.execute(db, """
        SELECT e.id AS entity_id, e.name AS '$(entity_class)'
        FROM entity e
        WHERE e.class_id = ?
        ORDER BY e.name
    """, [class_id]) |> DataFrame

    # Build deduplicated column names keyed by position (handles e.g. unit__node__node)
    col_names = _dedup_dim_col_names(dim_result[!, :dim_class])
    positions  = dim_result[!, :position]
    pos_to_col = Dict(positions[i] => col_names[i] for i in eachindex(col_names))

    if nrow(entities_df) == 0
        result = DataFrame()
        result[!, Symbol(entity_class)] = String[]
        for col in col_names
            result[!, Symbol(col)] = String[]
        end
        return result
    end

    # Fetch element member names with their position within the relationship
    entity_ids_str = join(entities_df[!, :entity_id], ",")
    elems_df = DBInterface.execute(db, """
        SELECT ee.entity_id, ee.position, e2.name AS member_name
        FROM entity_element ee
        JOIN entity e2 ON ee.element_id = e2.id
        WHERE ee.entity_id IN ($(entity_ids_str))
    """) |> DataFrame

    # Build mapping: entity_id → position → member_name
    elem_map = Dict{Int, Dict{Int, String}}()
    for row in eachrow(elems_df)
        if !haskey(elem_map, row.entity_id)
            elem_map[row.entity_id] = Dict{Int, String}()
        end
        elem_map[row.entity_id][row.position] = row.member_name
    end

    # Add one column per dimension in position order
    for (pos, col) in sort(collect(pos_to_col), by=first)
        entities_df[!, Symbol(col)] = [
            get(get(elem_map, entities_df[i, :entity_id], Dict{Int,String}()), pos, nothing)
            for i in 1:nrow(entities_df)
        ]
    end

    return select(entities_df, Not(:entity_id))
end
get_entity_names(
    db_path::AbstractString, entity_class::AbstractString
)::DataFrame = connect_db(db_path) do db
    return get_entity_names(db, entity_class)
end

"""
    get_parameter_definitions(db, [entity_class]) -> DataFrame

List available parameters for a given entity class (or all classes if not specified).

# Returns
A DataFrame with columns: parameter, entity_class, default_value, description, default_type.

`default_type` is the Spine type tag (e.g. `"float"`, `"time_series"`, `nothing` if no default).
For list enum defaults, it is enriched to `"list_value_ref{actual_type}"` (e.g. `"list_value_ref{str}"`).

`default_value` shows:
- Scalar defaults (`float`, `bool`, `str`) as their Julia value
- Enum defaults (`list_value_ref`) as the resolved string
- Compound defaults (`time_series`, `array`, `map`, `duration`) as a placeholder like `"<time_series>"`
- `nothing` when no default is set
"""
function get_parameter_definitions(db::SQLite.DB; entity_class::Union{Nothing, AbstractString}=nothing)
    if isnothing(entity_class)
        query = """
            SELECT pd.name AS parameter, ec.name AS entity_class, pd.description,
                   pd.default_type, pd.default_value
            FROM parameter_definition pd
            JOIN entity_class ec ON pd.entity_class_id = ec.id
            ORDER BY pd.name, ec.name
        """
        raw_df = DBInterface.execute(db, query) |> DataFrame
    else
        query = """
            SELECT pd.name AS parameter, ec.name AS entity_class, pd.description,
                   pd.default_type, pd.default_value
            FROM parameter_definition pd
            JOIN entity_class ec ON pd.entity_class_id = ec.id
            WHERE ec.name = ?
            ORDER BY pd.name
        """
        raw_df = DBInterface.execute(db, query, [entity_class]) |> DataFrame
    end

    decoded = Vector{Any}(undef, nrow(raw_df))
    enriched_type = Vector{Any}(undef, nrow(raw_df))
    for i in 1:nrow(raw_df)
        type_str = raw_df[i, :default_type]
        blob = raw_df[i, :default_value]
        decoded[i] = _decode_parameter_value(type_str, blob; db=db, detailed=false)
        enriched_type[i] = _enrich_type(type_str, blob, db)
    end
    raw_df[!, :default_value] = decoded
    raw_df[!, :default_type] = enriched_type

    return _nulls_to_nothing!(select(raw_df, [:parameter, :entity_class, :default_value, :description, :default_type]))
end
get_parameter_definitions(db_path::AbstractString)::DataFrame = connect_db(db_path) do db
    return get_parameter_definitions(db; entity_class=nothing)
end
get_parameter_definitions(
    db_path::AbstractString, entity_class::Union{Nothing, AbstractString}
)::DataFrame = connect_db(db_path) do db
    return get_parameter_definitions(db; entity_class=entity_class)
end

"""
    get_parameter_values(db, parameter_name) -> DataFrame

Return an overview of all stored values for `parameter_name`, one row per (entity, alternative).

# Columns (in order)
- `entity_class` — entity class name
- `entity` — full entity name
- `parameter_name` — parameter name (same as the argument)
- `alternative` — alternative name
- `value` — decoded value: scalars and `list_value_ref` fully resolved; compound types shown as `"<time_series>"` etc.
- `value_type` — type tag; `list_value_ref` enriched to `"list_value_ref{actual_type}"`
- One column per explicit dimension (named by dimension class, suffixed when repeated, e.g. `node_1`, `node_2`)
- `_direction` — implicit direction: `"from_<dim>"` or `"to_<dim>"` (e.g. `"from_node"`) when the entity class encodes a directional relationship; absent otherwise

# Example
```julia
get_parameter_values("SpineDB.sqlite", "balance_type")
get_parameter_values("SpineDB.sqlite", "unit_availability_factor")
```
"""
function get_parameter_values(db::SQLite.DB, parameter_name::AbstractString)
    main_query = """
        SELECT ec.name AS entity_class, e.name AS entity, a.name AS alternative,
               pv.type AS value_type, pv.value, pv.entity_id, pv.id AS value_id
        FROM parameter_value pv
        JOIN parameter_definition pd ON pv.parameter_definition_id = pd.id
        JOIN entity_class ec ON pv.entity_class_id = ec.id
        JOIN entity e ON pv.entity_id = e.id
        JOIN alternative a ON pv.alternative_id = a.id
        WHERE pd.name = ?
        ORDER BY e.name, a.name
    """
    df = DBInterface.execute(db, main_query, [parameter_name]) |> DataFrame

    if nrow(df) == 0
        return df
    end

    # Decode values and enrich value_type (overview mode: compound → placeholder)
    decoded = Vector{Any}(undef, nrow(df))
    enriched_type = Vector{Any}(undef, nrow(df))
    for i in 1:nrow(df)
        type_str = df[i, :value_type]
        blob = df[i, :value]
        decoded[i] = _decode_parameter_value(type_str, blob; db=db, detailed=false)
        enriched_type[i] = _enrich_type(type_str, blob, db)
    end
    df[!, :value] = decoded
    df[!, :value_type] = enriched_type

    # Discover dimension classes for each entity class; build per-class position→col mapping
    entity_classes = unique(df[!, :entity_class])
    entity_class_pos_to_col = Dict{String, Dict{Int, String}}()
    all_dim_cols = String[]

    for ec in entity_classes
        ec_result = DBInterface.execute(
            db, "SELECT id FROM entity_class WHERE name = ?", [ec]
        ) |> DataFrame
        if nrow(ec_result) == 0
            continue
        end
        ec_id = ec_result[1, :id]
        dim_result = DBInterface.execute(db, """
            SELECT ec2.name AS dim_class, ecd.position
            FROM entity_class_dimension ecd
            JOIN entity_class ec2 ON ecd.dimension_id = ec2.id
            WHERE ecd.entity_class_id = ?
            ORDER BY ecd.position
        """, [ec_id]) |> DataFrame
        if nrow(dim_result) > 0
            col_names = _dedup_dim_col_names(dim_result[!, :dim_class])
            positions  = dim_result[!, :position]
            pos_to_col = Dict(positions[i] => col_names[i] for i in eachindex(col_names))
            entity_class_pos_to_col[ec] = pos_to_col
            for col in col_names
                if col ∉ all_dim_cols
                    push!(all_dim_cols, col)
                end
            end
        end
    end

    # Fetch entity elements and build entity_id → col_name → member_name
    entity_id_to_class = Dict{Int,String}(df[i, :entity_id] => df[i, :entity_class] for i in 1:nrow(df))
    entity_dim_col_map = Dict{Int, Dict{String, String}}()
    if !isempty(all_dim_cols)
        entity_ids_str = join(unique(df[!, :entity_id]), ",")
        elems_df = DBInterface.execute(db, """
            SELECT ee.entity_id, ee.position, e2.name AS member_name
            FROM entity_element ee
            JOIN entity e2 ON ee.element_id = e2.id
            WHERE ee.entity_id IN ($(entity_ids_str))
        """) |> DataFrame
        for row in eachrow(elems_df)
            eid = row.entity_id
            ec  = get(entity_id_to_class, eid, nothing)
            isnothing(ec) && continue
            col = get(get(entity_class_pos_to_col, ec, Dict{Int,String}()), row.position, nothing)
            isnothing(col) && continue
            if !haskey(entity_dim_col_map, eid)
                entity_dim_col_map[eid] = Dict{String, String}()
            end
            entity_dim_col_map[eid][col] = row.member_name
        end

        for col in all_dim_cols
            df[!, Symbol(col)] = Vector{Any}([
                get(get(entity_dim_col_map, df[i, :entity_id], Dict{String,String}()), col, nothing)
                for i in 1:nrow(df)
            ])
        end
    end

    # Add implicit _direction column: extract "from_<dim>" / "to_<dim>" from class name
    has_from_to = any(ec -> occursin("__from_", ec) || occursin("__to_", ec), entity_classes)
    if has_from_to
        df[!, Symbol("_direction")] = Vector{Any}([
            begin
                m = match(r"__(from|to)_(.+)$", df[i, :entity_class])
                m !== nothing ? "$(m.captures[1])_$(m.captures[2])" : nothing
            end
            for i in 1:nrow(df)
        ])
    end

    # Add parameter_name column
    df[!, :parameter_name] .= parameter_name

    # Remove internal columns and reorder:
    # entity_class, entity, parameter_name, alternative, value, value_type, [dim cols], [_direction]
    select!(df, Not([:entity_id, :value_id]))
    base_cols = [:entity_class, :entity, :parameter_name, :alternative, :value, :value_type]
    dir_cols  = has_from_to ? [Symbol("_direction")] : Symbol[]
    return select(df, [base_cols; Symbol.(all_dim_cols); dir_cols])
end
get_parameter_values(
    db_path::AbstractString, parameter_name::AbstractString
)::DataFrame = connect_db(db_path) do db
    return get_parameter_values(db, parameter_name)
end

# Fallback: called without a parameter_name — inform the user and return nothing.
function get_parameter_values(_::SQLite.DB)
    @warn "get_parameter_values requires a parameter_name. " *
          "Use get_parameter_definitions(db) to list available parameters."
    return nothing
end
get_parameter_values(db_path::AbstractString) = connect_db(db_path) do db
    get_parameter_values(db)
end

"""
    get_parameter_value(db, parameter_name; entity, alternative, entity_class=nothing) -> Any

Return the fully decoded value for a single (parameter, entity, alternative) entry.

Use `entity_class` to disambiguate when the same parameter is defined on multiple entity
classes that share the same entity name. This commonly occurs for directional relationships
such as `unit__from_node`/`unit__to_node` or `connection__from_node`/`connection__to_node`.
If omitted and multiple rows match, an `@info` message lists the conflicting entity classes
and returns `nothing` — call with `entity_class=` set explicitly.

| Type            | Return                                          |
|-----------------|-------------------------------------------------|
| `float`         | `Float64`                                       |
| `bool`          | `Bool`                                          |
| `str`           | `String`                                        |
| `list_value_ref`| resolved `String`                               |
| `duration`      | `String` (e.g. `"10Y"`)                         |
| `time_series`   | `DataFrame` with columns `timestamp`, `value`   |
| `array`         | `Vector`                                        |
| `map`           | `Dict` / nested structure                       |

`alternative` must be specified explicitly — there is no default. If omitted, an `@info`
is emitted pointing to `get_alternatives(db)` and the function returns `nothing`.

Returns `nothing` if no matching row is found.

# Example
```julia
get_parameter_value("SpineDB.sqlite", "balance_type"; entity="OnGas", alternative="Base")
# → "balance_type_node"

get_parameter_value("SpineDB.sqlite", "unit_capacity";
    entity="PlfA1_BatterySto(I)", entity_class="unit__from_node")
```
"""
function get_parameter_value(
    db::SQLite.DB,
    parameter_name::AbstractString;
    entity::AbstractString,
    alternative::Union{Nothing, AbstractString}=nothing,
    entity_class::Union{Nothing, AbstractString}=nothing
)
    if isnothing(alternative)
        @warn "get_parameter_value: alternative not specified — use get_alternatives(db) " *
              "to see available alternatives, then re-call with alternative=\"...\"."
        return nothing
    end

    if isnothing(entity_class)
        query = """
            SELECT pv.type AS value_type, pv.value, ec.name AS entity_class
            FROM parameter_value pv
            JOIN parameter_definition pd ON pv.parameter_definition_id = pd.id
            JOIN entity_class ec ON pv.entity_class_id = ec.id
            JOIN entity e ON pv.entity_id = e.id
            JOIN alternative a ON pv.alternative_id = a.id
            WHERE pd.name = ? AND e.name = ? AND a.name = ?
        """
        result = DBInterface.execute(db, query, [parameter_name, entity, alternative]) |> DataFrame
    else
        query = """
            SELECT pv.type AS value_type, pv.value, ec.name AS entity_class
            FROM parameter_value pv
            JOIN parameter_definition pd ON pv.parameter_definition_id = pd.id
            JOIN entity_class ec ON pv.entity_class_id = ec.id
            JOIN entity e ON pv.entity_id = e.id
            JOIN alternative a ON pv.alternative_id = a.id
            WHERE pd.name = ? AND e.name = ? AND a.name = ? AND ec.name = ?
        """
        result = DBInterface.execute(db, query, [parameter_name, entity, alternative, entity_class]) |> DataFrame
    end

    if nrow(result) == 0
        return nothing
    end

    if nrow(result) > 1
        classes = join(["\"" * c * "\"" for c in result[!, :entity_class]], ", ")
        @warn "get_parameter_value: \"$parameter_name\" for entity \"$entity\" is ambiguous — " *
              "found in $(nrow(result)) entity classes: $classes.\n" *
              "Call with the entity_class keyword to select one,\n" *
              "e.g. get_parameter_value(..., \"$parameter_name\"; entity=\"$entity\", " *
              "entity_class=$(result[1, :entity_class]))"
        return nothing
    end

    return _decode_parameter_value(result[1, :value_type], result[1, :value]; db=db, detailed=true)
end
get_parameter_value(
    db_path::AbstractString,
    parameter_name::AbstractString;
    entity::AbstractString,
    alternative::Union{Nothing, AbstractString}=nothing,
    entity_class::Union{Nothing, AbstractString}=nothing
) = connect_db(db_path) do db
    return get_parameter_value(db, parameter_name;
        entity=entity, alternative=alternative, entity_class=entity_class)
end

# Fallback: called without a parameter_name — inform the user and return nothing.
function get_parameter_value(db::SQLite.DB; kwargs...)
    @warn "get_parameter_value requires a parameter_name. " *
          "Use get_parameter_definitions(db) to list available parameters."
    return nothing
end
get_parameter_value(db_path::AbstractString; kwargs...) = connect_db(db_path) do db
    get_parameter_value(db; kwargs...)
end