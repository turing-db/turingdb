graph [
  node [
    id 0
    label "patient_1"
    ethnicity "Black"
    type "Patient"
  ]
  node [
    id 1
    label "Black"
    type "Ethnicity"
  ]
  node [
    id 2
    label "hosp_42"
    type "Hospital"
  ]
  node [
    id 3
    label "Routine"
  ]
  node [
    id 4
    label "patient_2"
    ethnicity "Other"
    type "Patient"
  ]
  node [
    id 5
    label "Other"
    type "Ethnicity"
  ]
  node [
    id 6
    label "hosp_131"
    type "Hospital"
  ]
  node [
    id 7
    label "patient_3"
    ethnicity "White"
    type "Patient"
  ]
  node [
    id 8
    label "White"
    type "Ethnicity"
  ]
  node [
    id 9
    label "hosp_119"
    type "Hospital"
  ]
  node [
    id 10
    label "patient_4"
    ethnicity "White"
    type "Patient"
  ]
  edge [
    source 0
    target 1
    type "hasEthnicity"
  ]
  edge [
    source 0
    target 2
    type "wentTo"
  ]
  edge [
    source 0
    target 3
    type "visitReason"
  ]
  edge [
    source 3
    target 4
    type "visitReason"
  ]
  edge [
    source 3
    target 7
    type "visitReason"
  ]
  edge [
    source 3
    target 10
    type "visitReason"
  ]
  edge [
    source 4
    target 5
    type "hasEthnicity"
  ]
  edge [
    source 4
    target 6
    type "wentTo"
  ]
  edge [
    source 7
    target 8
    type "hasEthnicity"
  ]
  edge [
    source 7
    target 9
    type "wentTo"
  ]
  edge [
    source 8
    target 10
    type "hasEthnicity"
  ]
]
