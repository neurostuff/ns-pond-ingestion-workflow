# What the coordinate parse leaves out

`parse/coordinate_parse.json` (study_schema's `CoordinateParse`) records every table it did not
read as a `TableReading` with a `reason`, and every analysis it did not keep as an omission
with a `reason`. Nothing is dropped without one.

| Table reading | Reason |
|---|---|
| `excluded_by_hand` | the person's reason and note |
| `rejected_by_triage` | the triage route |
| `no_coordinates` | for a rodent atlas table: "its coordinates are rodent stereotaxic positions (bregma-relative or in a rodent atlas's axes), not brain coordinates in a template space" |

A table is a rodent atlas table when its caption, footer or column headings (never its rows)
either name a rodent atlas ("Paxinos and Watson", "Franklin and Paxinos"), or name an animal
(rat, mice, rodent, murine, "mouse brain", Sprague-Dawley, Wistar, Long-Evans, C57BL) together
with bregma, "interaural", Paxinos or Franklin, or all three of the AP, ML and DV axes. A bare
"mouse" is not an animal here, and human atlas references ("Mai and Paxinos") do not count.
Each analysis of such a table is omitted with the same reason; a table a person excluded or
triage rejected keeps that reading.
