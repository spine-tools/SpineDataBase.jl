"""
Examples to use functions in `SpineDataBase.jl`.
The `example.sqlite` file represents a SpineOpt input database.
"""

cd(@__DIR__)
import Pkg; Pkg.activate(@__DIR__)

Pkg.instantiate()
using SpineDataBase

db_path = "example.sqlite"

# 1: get lists of entities and alternatives
get_alternatives(db_path)
get_entity_classes(db_path)

# 2.1: entity names — simple object class (single column)
get_entity_names(db_path, "unit")

# 2.2: entity names — relationship class (full name + dimension columns)
get_entity_names(db_path, "unit__from_node")
get_entity_names(db_path, "unit__to_node")
get_entity_names(db_path, "unit__node__node")

# 3: parameter definitions with default_value column
get_parameter_definitions(db_path)
get_parameter_definitions(db_path, "node")
get_parameter_definitions(db_path, "unit__to_node")
get_parameter_definitions(db_path, "unit__node__node")
    
# 4.1: overview of all stored values for a parameter
get_parameter_values(db_path)   # warning on missing input arguments
get_parameter_values(db_path, "demand")
get_parameter_values(db_path, "balance_type")
get_parameter_values(db_path, "fix_ratio_out_in_unit_flow")
get_parameter_values(db_path, "unit_capacity")
get_parameter_values(db_path, "unit_capacity")[:, 2:end].unit

# 4.2: read a value defined on single-entity_class entity (parameter, entity, alternative)
## e.g. duration
get_parameter_value(db_path, "resolution"; entity="OprHrlyFlat", alternative="Base")   
## e.g. time_series (TimeSeries) fixed resolution
get_parameter_value(db_path, "demand"; entity="elecD", alternative="Base")

## warning on missing input arguments
get_parameter_value(db_path)    
get_parameter_value(db_path, "demand"; entity="elecD")    
get_parameter_value(db_path, "resolution"; entity="OprHrlyFlat")  

# 4.3: read a value defined on multi-entity_class entity (parameter, entity, alternative, entity_class)
## e.g. array
get_parameter_value(
    db_path, "fix_ratio_out_in_unit_flow"; 
    entity_class="unit__node__node", entity="DR__DRsource__elecD", alternative="Base"
)
## e.g. time_series (TimeSeries) fixed resolution
get_parameter_value(
    db_path, "unit_capacity"; 
    entity_class="unit__to_node", entity="DR__elecD", alternative="Base"
)
## e.g. time_series (TimeSeries) variable resolution
get_parameter_value(
    db_path, "vom_cost"; 
    entity_class="unit__to_node", entity="market__elecD", alternative="Base"
)

# 4.4: Return nothing and give info at ambiguous indices: unit__to/from_node, connection__to/from_node 
get_parameter_value(db_path, "unit_capacity"; entity="DR__elecD", alternative="Base")

