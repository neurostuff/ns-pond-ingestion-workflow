"""`compilation_of`: which titles name a meeting's abstracts published as one item.

The compilations are the 38 base studies deactivated on production Neurostore on 2026-11-26
and others the rule found in the production catalog. The studies are single abstracts and
papers in that catalog whose titles name a meeting, a poster or an abstract, and papers that
study conference abstracts; the last few are written to read like one and are not real.
"""

import pytest
from ingestion_workflow.pipeline.stages.sync import compilation_of

COMPILATIONS = [
    "ACNP 57th Annual Meeting: Poster Session I",
    "ACNP 56th Annual Meeting: Poster Session II, December 5, 2017",
    "ACNP 55th Annual Meeting: Poster Session IDecember 5, 2016",
    "ACNP 62nd Annual Meeting: Poster Abstracts P1 – P250",
    "ACNP 64th Annual Meeting: Poster Abstracts P292-583",
    "ACNP 58th Annual Meeting: Panels, Mini-Panels and Study Groups",
    "Poster Session IITuesday, December 8, 2015",
    "Poster Session III-WednesdayWednesday, December 11, 2013",
    "Poster Session II",
    "Posters",
    "Poster",
    "Poster Presentations",
    "Free Communications",
    "Awards",
    "BNA 2021 Festival of Neuroscience Poster abstracts",
    "16th European Headache Congress 2022 meeting abstracts",
    "Abstracts of the 7th World Parkinson Congress",
    "Abstracts From the 2016 Annual Meeting",
    "Abstracts from the 21st Annual Meeting of the North American Neuromodulation Society, "
    "Las Vegas, NV, USA January 11‐14, 2018",
    "Abstracts From The 5th Annual Conference Of The Canadian Neuromodulation Society, "
    "June 10 to 12, 2011, Vancouver, British Columbia, Canada",
    "Abstracts of Presentations at the International Conference on Basic and Clinical "
    "Multimodal Imaging (BaCI), a Joint Conference of the International Society for "
    "Neuroimaging in Psychiatry (ISNIP)",
    "Proceedings to the 59th Annual Conference of the Particle Therapy Cooperative Group "
    "(PTCOG59 2021 Online)",
    "Final Program, Thirty-Seventh Annual Meeting International Neuropsychological Society",
    "Annual Meeting of the Swiss Society of Clinical Neurophysiology, Basel, Switzerland, "
    "22–24 April 2004",
    "17th Clinical Trials on Alzheimer's Disease (CTAD) Madrid, Spain, October 29 - "
    "November 1, 2024: Poster Presentations.",
    "16^th Clinical Trials on Alzheimer’s Disease (CTAD) Boston, MA (USA) October 24–27, "
    "2023: Posters",
    "134th Annual Meeting Poster Session Abstracts Monday, October 12, 2009",
    "Brain PET Poster Sessions PP01-M01 to PP02-N07",
    "Free Communications (Poster Sessions)",
    "Proceedings of the 101st Annual Meeting of The Physiological Society of Japan",
    "Royal Osteoporosis Society, Osteoporosis Online Conference December 1st 2020: Abstracts",
    "ASENT2021 Annual Meeting Abstracts",
]

STUDIES = [
    # Single abstracts, each under its own number.
    "Poster 261: Increased Functional Connectivity between Pain‐Affect and Body‐Perception "
    "Brain Regions in Fibromyalgia",
    "AbstractAlzheimer’s imaging consortium presentation: Poster presentation: "
    "PosterIC-P-088: Reduced medial parietal activity in cognitively normal older adults",
    "P127: Subjective memory ability correlates with functional connectivity between the "
    "hippocampus and posterior default mode network",
    "Poster Number: NR 5 - Benzodiazepine Use Reduces Cortical Beta-Amyloid Levels and is "
    "Not Associated with Progressive Cognitive Decline",
    "AB0478 Comparative study between patients with normal and overweight in a cohort of "
    "systemic lupus erythematosus",
    "Poster 68",
    # Papers that study conference abstracts.
    "An Analysis of the Abstracts Presented at the Annual Meetings of the Society for "
    "Neuroscience from 2001 to 2006",
    "Analysis of subsequent publication of scientific orally presented abstracts of the French "
    "national congress of radiology",
    "Machine learning models can predict subsequent publication of North American Spine Society "
    "(NASS) annual general meeting abstracts",
    "Sex Distribution of Study Samples Reported in American Society of Biomechanics Annual "
    "Meeting Abstracts",
    "Trial registration of abstracts from the American Society of Anesthesiologists Meetings "
    "2010–2016: A review of prospective trials",
    "Using ClinicalTrials.gov to Supplement Information in Ophthalmology Conference Abstracts "
    "about Trial Outcomes: A Comparison Study",
    # Papers whose titles name a meeting, a session, a programme or an abstract.
    "Abstract linguistic structure correlates with temporal activity during naturalistic "
    "comprehension.",
    "Abstract reward and punishment representations in the human orbitofrontal cortex",
    "Summary of the Fibromyalgia Research Symposium 2016 in Nagasaki",
    "Meeting George Bush versus Meeting Cinderella: The Neural Response When Telling Apart "
    "What is Real from What is Fictional in the Context of Our Reality",
    "Cerebral Glucose Utilization Is Reduced in Second Test Session",
    "Poor between‐session recall of extinction learning and hippocampal activation and "
    "connectivity in children",
    "Systems, Subjects, Sessions: To What Extent Do These Factors Influence EEG Data?",
    "Posterior default mode network is associated with the social performance in male "
    "children with autism spectrum disorder",
    "Neural Responses to Elements of a Web-Based Smoking Cessation Program",
    # Not real: written to read like a compilation's title.
    "Posters in the waiting room increase vaccination uptake",
    "Poster presentations improve learning in medical students",
    "Abstracts of randomized trials frequently omit harms",
    "Abstracts from conference presentations rarely become papers",
    "Oral presentations of the self in schizophrenia: an fMRI study",
    "Awards and recognition in academic medicine",
    "Program evaluation of a workshop for caregivers",
    "",
    None,
]


@pytest.mark.parametrize("title", COMPILATIONS)
def test_a_meetings_abstracts_published_as_one_item_are_named(title):
    assert compilation_of(title)


@pytest.mark.parametrize("title", STUDIES)
def test_a_study_is_not(title):
    assert compilation_of(title) is None
