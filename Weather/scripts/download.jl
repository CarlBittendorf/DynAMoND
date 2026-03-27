include("../startup.jl")

using CSV

df_bursts = @chain d"DynAMoND Bergen Forms" begin
    transform(:FormTrigger => ByRow(Date) => :Date)

    groupby([:Participant, :Date])
    combine(All() => ((x...) -> true) => :IsBurst)
end

df = @chain d"DynAMoND Bergen Locations" begin
    # average location for each hour
    groupby_period(Hour(1); groupcols = [:Participant])
    combine([:Latitude, :Longitude] .=> mean; renamecols = false)

    transform(
        [:Latitude, :Longitude] .=> ByRow(x -> round(x; digits = 2));
        renamecols = false
    )

    # use only days within bursts
    transform(:DateTime => ByRow(Date) => :Date)
    leftjoin(df_bursts; on = [:Participant, :Date])
    dropmissing(:IsBurst)

    select(:Participant, :DateTime, :Latitude, :Longitude)
end

hourly = ["temperature_2m", "rain,precipitation", "apparent_temperature",
    "cloud_cover", "relative_humidity_2m", "wind_speed_10m"]

daily = ["daylight_duration", "sunshine_duration",
    "precipitation_sum", "rain_sum", "temperature_2m_mean"]

df_download = download(OpenMeteoHistoricalWeather, df; hourly, daily)

vestland = NaturalEarth.states() |>
           Filter(row -> !ismissing(row.name)) |>
           Filter(row -> row.name in ["Hordaland", "Sogn og Fjordane"]) |>
           Select(121) # geometry

df_weather = @chain df_download begin
    select(Not([:Date, :DateTimeHourly]))
    rename(_, map(x -> join(uppercasefirst.(split(x, "_"))), names(_)))

    groupby([:Participant, :DateTime])
    combine(All() .=> (x -> coalesce(x...)); renamecols = false)
end

df_vestland = @chain df_weather begin
    # use only points within Vestland (county in Norway)
    georef((:Longitude, :Latitude))
    geojoin(vestland; pred = ∈, kind = :inner)

    DataFrame
    transform(All() => ByRow((x...) -> true) => :Vestland)

    select(:Participant, :DateTime, :Vestland)
end

@chain df_weather begin
    leftjoin(df_vestland; on = [:Participant, :DateTime])
    transform(:Vestland => ByRow(!ismissing); renamecols = false)

    select(Not(:Latitude, :Longitude, :GridLatitude, :GridLongitude))
    sort([:Participant, :DateTime])

    CSV.write(joinpath("data", "Open-meteo Bergen Weather.csv"), _)
end