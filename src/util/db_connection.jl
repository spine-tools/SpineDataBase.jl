"""
Database utility functions for SpineOpt databases.
"""

using SQLite

"""
    connect_db(db_path::AbstractString) -> SQLite.DB

Open a connection to a SpineOpt output SQLite database.

# Arguments
- `db_path`: Path to the .sqlite database file

# Returns
An open SQLite.DB connection. Remember to close with `DBInterface.close!(db)`.

# Example
```julia
db = connect_db("Output.sqlite")
...
DBInterface.close!(db)
```
"""
function connect_db(db_path::AbstractString)::SQLite.DB
    if !isfile(db_path)
        error("Database file not found: $db_path")
    end
    return SQLite.DB(db_path)
end

"""
    connect_db(f::Function, db_path::AbstractString)

Open a database connection, execute function `f` with the connection,
then automatically close the connection.
Enable the do-block pattern for automatic cleanup:

# Example
```julia
connect_db("Output.sqlite") do db
    ...
end
```
"""
function connect_db(f::Function, db_path::AbstractString)
    db = connect_db(db_path)
    try
        return f(db)
    finally
        DBInterface.close!(db)
    end
end