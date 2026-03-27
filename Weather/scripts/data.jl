include("../startup.jl")

using CSV

CSV.write(joinpath("data", "DynAMoND Bergen Forms.csv"), d"DynAMoND Bergen Forms")