push!(LOAD_PATH, "../src/")
using Documenter
using SpineDataBase

makedocs(
    sitename = "SpineDataBase.jl",
    format = Documenter.HTML(),
    modules = [SpineDataBase],
    pages = [
        "Home" => "index.md",
        "Manual" => "manual.md",
        "API" => "api.md"
    ]

)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
deploydocs(
    repo = "https://github.com/spine-tools/SpineDataBase.jl.git",
    branch = "gh-pages",
    devbranch = "main"
)