function (; var"data#DynAMoND Forms")
    @chain var"data#DynAMoND Forms" begin
        subset(
            :StudyCenter => ByRow(isequal("Bergen")),
            :Form => ByRow(isequal("Burst"))
        )
        dropmissing(:Participant)

        groupby(:Participant)
        transform(:FormTrigger => enumerate_days => :Day; ungroup = false)

        transform([:MDMQContentMoment, :MDMQUnwellMoment] => ByRow((c, u) -> (abs(c - 100) + u) / 2) => :ValenceMoment)

        select(:Participant, :FormTrigger, :FormStart, :FormFinish,
            :IsMissing, :ReasonForMissing, :ValenceMoment)
    end
end