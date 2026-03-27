function (;
        var"data#DynAMoND m-Path Locations",
        var"data#DynAMoND movisensXS Mobile Sensing",
        var"data#DynAMoND Assignments",
        var"data#DynAMoND Connections",
        var"data#DynAMoND Bergen Forms",
        max_velocity = 300
)
    dict = Dict(
        12924 => "Genève",
        12944 => "Frankfurt",
        12949 => "Brescia",
        12951 => "Barcelona",
        13081 => "Bergen"
    )

    df_movisens = @chain var"data#DynAMoND movisensXS Mobile Sensing" begin
        gather(MovisensXSLocation)

        transform(:MovisensXSStudyID => ByRow(x -> dict[parse(Int, x)]) => :StudyCenter)
        subset(:StudyCenter => ByRow(isequal("Bergen")))

        leftjoin(
            dropmissing(var"data#DynAMoND Assignments", :MovisensXSParticipantID);
            on = [:StudyCenter, :MovisensXSParticipantID]
        )
        dropmissing(:Participant)
    end

    df_mpath = @chain var"data#DynAMoND m-Path Locations" begin
        leftjoin(var"data#DynAMoND Connections"; on = :MPathConnectionID)

        subset(:StudyCenter => ByRow(isequal("Bergen")))
        dropmissing(:Participant)

        transform(:LocationDateTime => identity => :DateTime)
        select(Not(:LocationDateTime))
        rename(:LocationAccuracy => :LocationConfidence)
    end

    participants = unique(var"data#DynAMoND Bergen Forms".Participant)

    @chain begin
        vcat(df_movisens, df_mpath; cols = :intersect)

        subset(:Participant => ByRow(x -> x in participants))

        sort([:Participant, :DateTime])

        filter_locations(; max_velocity, groupcols = [:Participant])
        fill_periods(Day(1), Minute(1); groupcols = [:Participant])

        groupby(:Participant)
        transform([:Latitude, :Longitude] .=> fill_down; renamecols = false)

        select(:Participant, :DateTime, :Latitude, :Longitude)
        sort([:Participant, :DateTime])
    end
end