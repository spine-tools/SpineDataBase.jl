module SpineDataBase

"""
Author: Huang, JiangYi (nnhjy <43530784+nnhjy@users.noreply.github.com>)
Date: 2026-Apr-23

Utilities to access data from a Spine database.
"""

using SQLite
using DataFrames
using JSON
using Dates

include("./util/db_connection.jl")
include("./util/db_decoding.jl")
include("./main_query_functions.jl")

export connect_db

export get_alternatives
export get_entity_names, get_entity_classes
export get_parameter_definitions
export get_parameter_values, get_parameter_value

end # module SpineDataBase
