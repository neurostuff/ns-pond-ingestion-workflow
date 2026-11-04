"""Route A of canonical-task assignment: lexical match of a mention's name / paradigm_term against a seed lexicon.

Lexicon = hed-task canonical names, aliases, variation names (-> hedtsk/hedvar ids)
        + Cognitive Atlas task names with an exact/close hed-task crosswalk (data/mappings/atlas_task_to_hed.tsv)
        + regex patterns for frequent surface forms (n-back with digits, go/no-go spellings, ...)
        + corpus extensions (tct_*) for frequent paradigms hed-task lacks.
Longest normalized-token match wins; ties broken by source priority (hed name > alias > variation > CA > regex).
"""
import csv
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
HED = os.path.join(HERE, "..", "data", "hed-task")

# Extensions: frequent paradigms in the neuroimaging corpus that hed-task (107) lacks.
# id -> (canonical name, family, patterns). Families reuse hed-task family ids.
EXTENSIONS = {
    "tct_cue_reactivity": ("Cue Reactivity Task", "emotion", [r"cue[- ]?(reactivity|exposure)", r"craving (cue|induction)",
                           r"(drug|alcohol|smoking|cigarette|food|cocaine|cannabis|gambling)[- ]cue"]),
    "tct_emotional_face_matching": ("Emotional Face Matching Task", "emotion", [r"(emotional |hariri |face[- ])?(face|faces)[- ]matching",
                                    r"emotional reactivity", r"faces?[- ]shapes?"]),
    "tct_pain_stimulation": ("Pain Stimulation", "perceptual_judgment", [r"(painful|noxious|nociceptive|thermal|heat|pain)[- ]?(stimulation|stimuli|stimulus)",
                             r"\bpain (task|paradigm|processing|perception)", r"cold pressor"]),
    "tct_naturalistic_viewing": ("Naturalistic Movie Viewing", "perceptual_judgment", [r"(movie|film|video)[- ]?(clip )?(watching|viewing)",
                                 r"naturalistic (viewing|stimul)", r"watch(ed|ing)? (a )?(movie|film)"]),
    "tct_story_listening": ("Narrative Listening", "language", [r"(story|narrative|speech) (listening|comprehension)", r"listen(ed|ing)? to (a |the )?stor"]),
    "tct_semantic_judgment": ("Semantic Judgment Task", "language", [r"semantic (judg|decision|categori[sz]ation|classification|association)",
                              r"(abstract|concrete)[-/ ](concrete|abstract) (judg|decision)", r"living[-/ ]non[- ]?living"]),
    "tct_phonological_judgment": ("Phonological/Rhyme Judgment Task", "language", [r"rhym(e|ing) (judg|decision|task)", r"phonological (judg|decision)"]),
    "tct_action_observation": ("Action Observation Task", "motor_performance", [r"action[- ]observation", r"observ(e|ing|ation of) (hand )?(actions|movements)"]),
    "tct_motor_imagery": ("Motor Imagery Task", "motor_performance", [r"motor imagery", r"imagined (movement|grasping)", r"kinesthetic imagery"]),
    "tct_motor_execution": ("Simple Motor Execution Task", "motor_performance", [r"(hand|finger|foot|tongue|lip)[- ]?(movement|clench|flexion|opposition|motor)",
                            r"\bmotor (task|paradigm|localizer)", r"grip(ping)?[- ]?(force)?"]),
    "tct_theory_of_mind": ("Theory of Mind Task", "social_cognition_and_games", [r"theory[- ]of[- ]mind", r"\btom\b", r"mentali[sz]ing",
                           r"false[- ]photo", r"(animated|moving) (shapes|triangles)", r"frith[- ]happ"]),
    "tct_social_exclusion": ("Cyberball Social Exclusion", "social_cognition_and_games", [r"cyber[- ]?ball", r"social exclusion"]),
    "tct_episodic_encoding": ("Episodic Encoding / Subsequent Memory Task", "recall_and_recognition", [r"subsequent memory", r"(memory |episodic |incidental |intentional )encoding",
                              r"encoding (task|paradigm|phase)"]),
    "tct_working_memory_generic": ("Working Memory Task (unspecified)", "working_memory_span", [r"working[- ]memory (task|paradigm)", r"\bwm task"]),
    "tct_card_guessing": ("Card-Guessing (Gambling) Task", "reward_and_value_choice", [r"card[- ]guessing", r"gambling (task|paradigm)", r"guessing (task|game)", r"\bgambling\b"]),
    "tct_reward_feedback": ("Reward Processing Task (unspecified)", "reward_and_value_choice", [r"reward (task|paradigm|processing)"]),
    "tct_fear_extinction": ("Fear Extinction Task", "conditioning_and_reinforcement", [r"extinction (learning|recall|training|task|paradigm)"]),
    "tct_retinotopy": ("Retinotopic Mapping", "perceptual_judgment", [r"retinotop", r"(rotating )?wedge", r"expanding ring", r"meridian mapping",
                       r"population receptive field|\bprf\b"]),
    "tct_visual_localizer": ("Visual Category Localizer", "perceptual_judgment", [r"\b(ffa|ppa|loc|eba|vwfa|mt\+?|hmt)\b.{0,15}locali[sz]er",
                             r"(face|place|scene|body|object|category|visual)[- ]locali[sz]er", r"functional locali[sz]er"]),
    "tct_checkerboard": ("Flickering Checkerboard Stimulation", "perceptual_judgment", [r"checker[- ]?board"]),
    "tct_auditory_stimulation": ("Passive Auditory Stimulation", "perceptual_judgment", [r"passive (auditory|listening)", r"auditory stimulation",
                                 r"(tone|sound|music) (listening|presentation)", r"music listening"]),
    "tct_imitation": ("Imitation Task", "motor_performance", [r"\bimitation\b"]),
    "tct_mental_arithmetic": ("Mental Arithmetic Task", "executive_and_reasoning", [r"(mental )?arithmetic", r"calculation (task|paradigm)", r"(addition|subtraction|multiplication) task",
                              r"number (comparison|magnitude)", r"numerical (comparison|magnitude)"]),
    "tct_self_referential": ("Self-Referential Judgment Task", "memory_control", [r"self[- ]referen", r"self[- ]reflect", r"trait (judg|adjective)"]),
    "tct_empathy_pain_observation": ("Empathy for Pain Task", "social_cognition_and_games", [r"empath", r"pain (observation|empathy)", r"others'? pain"]),
    "tct_inhibition_generic": ("Response Inhibition (unspecified)", "response_inhibition", [r"response inhibition (task|paradigm)", r"inhibitory control (task|paradigm)"]),
    "tct_msit": ("Multi-Source Interference Task", "conflict_and_interference", [r"multi[- ]?source interference", r"\bmsit\b"]),
    "tct_interference_generic": ("Conflict/Interference (unspecified)", "conflict_and_interference", [r"(cognitive )?(conflict|interference) (task|paradigm)"]),
    "tct_moral_judgment": ("Moral Judgment Task", "social_cognition_and_games", [r"moral (judg|dilemma|decision|reasoning)"]),
    "tct_risky_decision": ("Risky Decision-Making Task", "reward_and_value_choice", [r"risk(y)? (decision|choice|taking|gain)", r"(lottery|gamble) choice", r"cups task",
                           r"loss aversion", r"mixed gambles?"]),
    "tct_speech_production": ("Overt Speech Production Task", "language", [r"speech production", r"(overt|covert) (naming|speech|reading)", r"word (repetition|generation)",
                              r"sentence (production|generation)"]),
    "tct_reading": ("Reading Task", "language", [r"\breading (task|paradigm|comprehension|fluency|test)", r"(sentence|word|text) reading", r"silent reading"]),
    "tct_neurofeedback": ("Neurofeedback Training", "metacognition_and_interoception", [r"neuro[- ]?feedback"]),
    "tct_breath_hold": ("Breath-Hold / Respiratory Challenge", "pseudo_tasks", [r"breath[- ]?hold", r"hypercapni", r"respiratory challenge"]),
    "tct_sleep": ("Sleep Recording", "pseudo_tasks", [r"\bsleep (recording|deprivation|stages?)\b", r"\bnap\b"]),
    "tct_dichotic_listening": ("Dichotic Listening Task", "perceptual_judgment", [r"dichotic"]),
    "tct_stress_induction": ("Psychosocial Stress Induction", "emotion", [r"montreal imaging stress", r"\bmist\b", r"trier social stress", r"stress (induction|task|paradigm)"]),
    "tct_time_estimation": ("Time Estimation/Interval Timing Task", "motor_performance", [r"time (estimation|perception|reproduction|discrimination)", r"temporal (discrimination|bisection)", r"interval timing", r"duration (judg|discrimination)"]),
    "tct_deception": ("Deception Task", "social_cognition_and_games", [r"decept", r"\blying\b", r"concealed information|guilty knowledge"]),
    "tct_approach_avoidance": ("Approach-Avoidance Task", "emotion", [r"approach[- ]avoid"]),
    "tct_creativity": ("Divergent Thinking/Creativity Task", "executive_and_reasoning", [r"alternate uses|alternative uses|divergent thinking|creativ"]),
    "tct_sensory_stimulation": ("Passive Somatosensory/Chemosensory Stimulation", "perceptual_judgment", [r"(tactile|vibrotactile|somatosensory|olfactory|gustatory|taste|odou?r|median nerve|acupuncture) (stimulation|stimuli)", r"acupuncture"]),
    "tct_saccade": ("Saccade Task (pro/visually guided)", "motor_performance", [r"\bsaccad", r"smooth pursuit", r"oculomotor"]),
    "tct_wheel_of_fortune": ("Wheel of Fortune Task", "reward_and_value_choice", [r"wheel of fortune"]),
    # v3 additions (2026-10-09 01:20): recurring gold NEW procedures + largest residue clusters
    "tct_implicit_emotional_face": ("Implicit Emotional Face Processing (incidental judgment)", "emotion", [r"(gender|sex|age|color|colour) (judg|discrimination|decision|identification|categori[sz]ation)\w* (of|on|task)?.{0,20}(emotional )?faces?", r"implicit (emotion|emotional face|affect)", r"gender (judg|discrimination|decision|identification)"]),
    "tct_threat_of_shock": ("Threat-of-Shock / Threat Anticipation", "emotion", [r"threat[- ]of[- ]shock", r"\bnpu\b", r"threat (anticipation|cue)", r"instructed (fear|threat)", r"anticipat\w+ (of )?(shock|threat|pain)"]),
    "tct_affective_anticipation": ("Cued Affective Picture Anticipation", "emotion", [r"anticipation of (emotional|aversive|negative)", r"emotional anticipation", r"expectancy (task|paradigm)"]),
    "tct_simple_choice_rt": ("Simple/Choice Reaction Time Task", "motor_performance", [r"(simple|choice|two[- ]choice|four[- ]choice) reaction[- ]time"]),
    "tct_visual_reproduction": ("Visual Reproduction / Figure Recall (drawing from memory)", "recall_and_recognition", [r"visual reproduction", r"rey[- ]osterrieth", r"complex figure", r"\brcft\b", r"benton visual retention"]),
    "tct_auditory_discrimination": ("Auditory Psychophysical Discrimination (pitch, duration, loudness)", "perceptual_judgment", [r"pitch (discrimination|judg|identification|processing)", r"(tone|frequency|duration|loudness|timbre) discrimination", r"melod(y|ic) (discrimination|comparison)"]),
    "tct_sound_categorization": ("Sound / Voice Categorization Task", "perceptual_judgment", [r"sound categori", r"(voice|vocal) (localizer|categori|perception)", r"environmental sound", r"sound (identification|recognition)"]),
    "tct_appetitive_conditioning": ("Appetitive (Reward) Pavlovian Conditioning", "conditioning_and_reinforcement", [r"appetitive (conditioning|pavlovian)", r"reward conditioning", r"pavlovian[- ]instrumental transfer|\bpit\b task"]),
    "tct_picture_word_interference": ("Picture-Word Interference Task", "language", [r"picture[- ]word interference", r"\bpwi\b"]),
    "tct_cancellation": ("Cancellation Test", "visual_search_and_tracking", [r"cancell?ation (test|task)", r"letter cancell?ation", r"bells test"]),
    "tct_wm_updating": ("Working Memory Updating (running span / keep-track)", "working_memory_span", [r"running (memory|span)", r"keep[- ]track", r"memory updating", r"letter memory"]),
    "tct_writing": ("Writing / Spelling Task", "language", [r"writing (to dictation|task)|handwriting|spelling (task|to dictation)|\bdictation\b"]),
    "tct_valence_judgment": ("Evaluative / Valence Judgment Task", "emotion", [r"(valence|evaluative|affective|emotional|pleasantness) (judg|rating|categori|evaluation)", r"(like|dislike) judg"]),
    "tct_mood_induction": ("Mood / Emotion Induction", "emotion", [r"(mood|emotion|sadness|anger|fear) induction", r"script[- ]driven (imagery)?", r"(emotional|autobiographical) script", r"emotion(al)? (elicitation|recall) (task|paradigm)"]),
    "tct_animal_behavior": ("Animal Behavioral Test (rodent/NHP)", "animal_behavior", [r"rotarod", r"beam[- ]walk", r"open[- ]field", r"sucrose preference", r"forced swim", r"tail suspension", r"elevated plus", r"passive avoidance", r"novel object recognition", r"fear[- ]potentiated startle in (rats|mice)", r"barnes maze", r"y[- ]maze", r"t[- ]maze", r"light[- ]dark box"]),
    "tct_perceptual_decision": ("Perceptual Decision / Discrimination (2AFC)", "perceptual_judgment", [r"perceptual (decision|discrimination|judg)", r"(orientation|contrast|motion direction|color|colour|shape|size|speed) (discrimination|judg)", r"two[- ]alternative forced[- ]choice|\b2afc\b|same[/ -]different"]),
    "tct_category_learning": ("Category Learning Task", "conditioning_and_reinforcement", [r"categor(y|ization) learning", r"prototype (learning|distortion)", r"rule[- ]based (category|categorization)"]),
    "tct_categorization": ("Visual/Semantic Categorization Task", "perceptual_judgment", [r"\bcategori[sz]ation (task|paradigm)", r"(scene|object|animal|visual) categori[sz]ation"]),
    "tct_gaze_joint_attention": ("Gaze Perception / Joint Attention Task", "social_cognition_and_games", [r"joint attention", r"gaze (cue|cueing|following|perception|direction|processing)", r"eye[- ]contact"]),
    "tct_humor": ("Humor Processing Task", "emotion", [r"humou?r", r"jokes?\b", r"cartoons? (funny|humor)"]),
    "tct_line_bisection": ("Line Bisection / Landmark Task", "perceptual_judgment", [r"line bisection", r"landmark (task|test)"]),
    "tct_visuomotor_adaptation": ("Visuomotor Adaptation / Tracking", "motor_performance", [r"visuo[- ]?motor (adaptation|learning|tracking|rotation|coordination)", r"prism adaptation", r"force[- ]field adaptation", r"(manual|pursuit) tracking"]),
    "tct_gesture": ("Gesture Observation / Production Task", "social_cognition_and_games", [r"gestur", r"pantomim"]),
    "tct_audiovisual_integration": ("Audiovisual / Multisensory Integration Task", "perceptual_judgment", [r"audio[- ]?visual (integration|speech|matching|congruen)", r"multisensory integration", r"mcgurk", r"sound[- ]induced flash", r"temporal order judg"]),
    "tct_aesthetic_judgment": ("Aesthetic Judgment Task", "reward_and_value_choice", [r"aesthetic", r"beauty (judg|rating)", r"attractiveness (judg|rating)"]),
    "tct_virtual_navigation": ("Virtual Navigation / Wayfinding", "spatial_navigation", [r"(virtual|spatial|mental) navigation", r"wayfinding", r"path integration", r"route (learning|recall)", r"virtual (town|city|environment) (task|navigation)"]),
    "tct_mental_imagery": ("Mental Imagery Task", "spatial_navigation", [r"(visual|mental|guided|auditory) imagery", r"\bimagery (task|paradigm|condition)"]),
    "tct_sentence_completion": ("Sentence Completion (Hayling)", "language", [r"hayling", r"sentence completion"]),
    "tct_motion_perception": ("Visual Motion Perception Task", "perceptual_judgment", [r"(visual )?motion (perception|processing|discrimination|detection)", r"\bmt\+? (localizer|mapping)", r"optic flow"]),
    "tct_odor_test": ("Olfactory Threshold/Identification Test", "perceptual_judgment", [r"sniffin'? sticks", r"\bupsit\b", r"(odou?r|olfactory) (identification|threshold|discrimination) (test|task)"]),
    "tct_swallowing": ("Swallowing Task", "motor_performance", [r"swallow"]),
    "tct_grasping": ("Reach-to-Grasp Task", "motor_performance", [r"grasp", r"reach(ing)?[- ]to[- ]grasp", r"\breaching (task|movements?)"]),
    "tct_threat_social_evaluation": ("Social Evaluative Feedback Task", "social_cognition_and_games", [r"social (evaluat|feedback)", r"peer (feedback|evaluation)", r"chatroom", r"social judgment (task|paradigm)"]),
    "tct_risk_rating": ("Hazard / Risk Perception Rating", "reward_and_value_choice", [r"risk (perception|evaluation|rating)", r"hazard (perception|rating)"]),
    "tct_numerosity": ("Numerosity / Magnitude Comparison", "executive_and_reasoning", [r"numerosit", r"(magnitude|quantity|dot) comparison", r"approximate number"]),
    "tct_interception": ("Interception / Target Tracking Action Task", "motor_performance", [r"intercept", r"catch(ing)? (a )?(moving|ball)"]),
    "tct_sr_binding": ("Stimulus-Response Binding / Event-File Task", "conflict_and_interference", [r"s-?r binding", r"event[- ]file", r"partial repetition"]),
    "tct_hearing_check": ("Hearing Screening / Headphone Check", "pseudo_tasks", [r"headphone (check|screen)", r"audiometr", r"pure[- ]tone (average|threshold|audiometry)", r"hearing (screen|test|threshold)"]),
    "tct_comprehension_questions": ("Comprehension Questions (attention check)", "pseudo_tasks", [r"comprehension (questions?|check|statements?)", r"attention check"]),
    "tct_food_choice": ("Food Choice / Portion Selection Task", "reward_and_value_choice", [r"food (choice|selection|preference|decision)s?", r"portion (selection|size)", r"(shopping|grocery|menu) (task|choice)", r"dietary (choice|decision)"]),
    "tct_face_social_judgment": ("Social Judgment of Faces (attractiveness, trustworthiness)", "social_cognition_and_games", [r"(attractiveness|trustworthiness|dominance|competence) (judg|rating)", r"face (ratings?|evaluation)", r"first impressions?"]),
    "tct_stimulus_rating": ("Stimulus Rating Task (valence/arousal/liking/intensity)", "emotion", [r"(picture|image|photo|stimulus|product|word|music|odou?r) rating", r"rating task", r"affect grid", r"(arousal|valence|liking|intensity) ratings?"]),
    "tct_random_generation": ("Random Number/Sequence Generation", "executive_and_reasoning", [r"random[- ](number|sequence|interval)[- ]generation", r"random generation"]),
    "tct_infant_looking": ("Infant Looking-Time Paradigm (preferential looking, habituation, VoE)", "perceptual_judgment", [r"preferential looking", r"(novelty|familiarity) preference", r"habituation (paradigm|procedure|task)", r"violation[- ]of[- ]expectation", r"looking[- ]time"]),
    "tct_object_location_memory": ("Object-Location / Spatial Reconstruction Memory", "spatial_navigation", [r"object[- ](location|relocation|placement)", r"spatial reconstruction", r"location (memory|judg)"]),
    "tct_vocabulary_test": ("Vocabulary / Receptive Language Test", "language", [r"vocabulary (test|task|knowledge)", r"\bppvt\b|peabody", r"receptive (vocabulary|language)"]),
    "tct_visual_world": ("Visual World Eye-Tracking Paradigm", "language", [r"visual[- ]world (paradigm|task|eye)"]),
    "tct_serial_recall": ("Immediate Serial Recall / Serial-Order Memory", "working_memory_span", [r"(immediate )?serial (recall|recognition|order (memory|recall|recognition|reconstruction))", r"order reconstruction"]),
    "tct_delayed_estimation": ("Delayed Estimation / Continuous Report", "working_memory_span", [r"delayed estimation", r"continuous report", r"(orientation|colou?r) (reproduction|recall wheel)", r"what was where"]),
    "tct_face_identity_matching": ("Unfamiliar Face Identity Matching", "perceptual_judgment", [r"glasgow face matching", r"\bgfmt\b", r"face (identity )?matching test", r"(unfamiliar )?face (identity )?(comparison|matching) (task|test)"]),
    "tct_famous_face_recognition": ("Famous/Familiar Face Recognition", "recall_and_recognition", [r"famous (face|person)", r"familiar face recognition"]),
    "tct_opposite_naming": ("Opposite-Naming Inhibition (day-night)", "response_inhibition", [r"day[- ]night", r"opposite[- ]naming", r"grass[- ]snow", r"head[- ]toes[- ]knees"]),
    "tct_truth_rating": ("Statement Truth Rating (illusory truth)", "information_evaluation_and_belief", [r"illusory truth", r"truth (rating|judg)", r"(statement|headline) (truth|accuracy|veracity) (rating|judg)", r"truth effect"]),
    "tct_continued_influence": ("Continued Influence / Misinformation Task", "information_evaluation_and_belief", [r"continued[- ]influence", r"misinformation (task|paradigm|effect)", r"(news|headline) sharing"]),
    "tct_event_segmentation": ("Event Segmentation Task", "perceptual_judgment", [r"event segmentation", r"(natural|unitization) segmentation", r"event boundar"]),
    "tct_cognitive_reflection": ("Cognitive Reflection Test", "executive_and_reasoning", [r"cognitive reflection", r"\bcrt[- ]?2?\b(?=.*(reflection|test))|\bcrt[- ]2\b"]),
    "tct_food_intake": ("Ad Libitum Food Intake (taste test)", "ingestive_behavior", [r"ad[- ]?lib(itum)?", r"(bogus )?taste test", r"food intake (test|task|measure)", r"snack intake"]),
    "tct_dyadic_interaction": ("Dyadic / Parent-Infant Interaction (observed)", "dyadic_interaction_observation", [r"free[- ]play", r"(parent|mother|caregiver)[- ](infant|child) interaction", r"dyadic (interaction|communication|coordination)", r"joint action", r"conversation (task|paradigm)"]),
    "tct_aiming": ("Aiming / Pointing Movement Task (Fitts)", "motor_performance", [r"aiming (task|movements?)", r"\bfitts", r"(rapid|discrete) pointing", r"pointing (task|movements?)", r"center[- ]out reach"]),
    "tct_motor_skill_acquisition": ("Motor Skill Acquisition / Practice", "motor_performance", [r"(motor|sport|golf|throwing|putting) skill (acquisition|learning)", r"skill acquisition", r"(bimanual|polyrhythm) coordination"]),
    "tct_nonword_repetition": ("Nonword Repetition Task", "language", [r"non[- ]?word repetition", r"pseudoword repetition"]),
    "tct_word_learning": ("Novel Word Learning / Word-Object Mapping", "language", [r"(novel )?word[- ](object )?(learning|mapping)", r"cross[- ]situational", r"artificial (lexicon|language) learning", r"fast mapping"]),
    "tct_partial_report": ("Partial Report / Iconic Memory", "working_memory_span", [r"partial report", r"iconic memory", r"post[- ]cued (report|recall)"]),
    "tct_visual_pattern_span": ("Visual Patterns Span Test", "working_memory_span", [r"visual patterns? (test|span)", r"matrix span"]),
    "tct_self_ordered_search": ("Self-Ordered Spatial Search (CANTAB SWM)", "working_memory_span", [r"self[- ]ordered (search|pointing)", r"spatial working memory \(?swm", r"cantab swm"]),
    "tct_deductive_reasoning": ("Deductive / Relational Reasoning Task", "executive_and_reasoning", [r"(deductive|inductive|relational|analogical|syllogistic|transitive) (reasoning|inference)", r"syllogis", r"analog(y|ies) task"]),
    "tct_meditation": ("Meditation", "pseudo_tasks", [r"meditat", r"mindfulness (practice|exercise|state)"]),
}

import json as _json

_BM = os.path.join(HERE, "..", "data", "brainmap_crosswalk.json")
_BM_SKIP = {"tct_bm_counting_stroop", "tct_bm_spatial_stroop", "tct_bm_grammaticality_judgment"}
BRAINMAP_EXT, BRAINMAP_NAMES = {}, []
if os.path.exists(_BM):
    for _x in _json.load(open(_BM)):
        _pe = _x.get("proposed_extension")
        if _pe and _pe["id"] not in _BM_SKIP:
            BRAINMAP_EXT[_pe["id"]] = (_pe["name"], _pe["family"], _pe["patterns"])
        if _x["match_type"] in ("exact", "close") and _x.get("inventory_id"):
            BRAINMAP_NAMES.append((_x["brainmap_class"], _x["inventory_id"]))
EXTENSIONS_BM = BRAINMAP_EXT

# Regex surface forms for hed-task tasks that literal names miss.
HED_PATTERNS = {
    "hedtsk_n_back": [r"\b[0-4n][- ]?back\b", r"\bn[- ]?back", r"\b(zero|one|two|three)[- ]back"],
    "hedtsk_go_no_go": [r"go[ /-]?no[ /-]?go", r"go[ /-]nogo"],
    "hedtsk_stop_signal": [r"stop[- ]signal", r"stop[- ]?task"],
    "hedtsk_monetary_incentive_delay": [r"monetary incentive delay", r"\bmid (task|paradigm)", r"incentive delay"],
    "hedtsk_stroop_color_word": [r"stroop"],
    "hedtsk_eriksen_flanker": [r"flanker"],
    "hedtsk_oddball": [r"odd[- ]?ball", r"\bp300\b", r"target detection"],
    "hedtsk_mental_rotation": [r"mental rotation"],
    "hedtsk_delay_discounting": [r"(delay|temporal|intertemporal|effort|probability) discounting", r"intertemporal choice"],
    "hedtsk_iowa_gambling": [r"iowa gambling", r"\bigt\b"],
    "hedtsk_balloon_analog_risk": [r"balloon anal", r"\bbart\b"],
    "hedtsk_wisconsin_card_sorting": [r"wisconsin card", r"\bwcst\b", r"card[- ]sorting"],
    "hedtsk_lexical_decision": [r"lexical decision"],
    "hedtsk_verbal_fluency": [r"(verbal|semantic|phonemic|letter|category) fluency"],
    "hedtsk_verb_generation": [r"verb generation"],
    "hedtsk_picture_naming": [r"(picture|object|confrontation) naming"],
    "hedtsk_finger_tapping": [r"finger[- ]?(tapping|opposition|sequence)"],
    "hedtsk_emotion_regulation": [r"(emotion|emotional) regulation", r"reappraisal", r"(cognitive|emotion) suppression"],
    "hedtsk_affective_picture_viewing": [r"\biaps\b", r"(emotional|affective|aversive) (picture|image|scene)s?( viewing| task| processing)?"],
    "hedtsk_facial_emotion_recognition": [r"(facial )?emotion(al)? (recognition|identification|labeling)", r"(facial )?expression (recognition|identification)"],
    "hedtsk_face_processing": [r"face (processing|perception|viewing)", r"(emotional |fearful )?faces? (task|paradigm)"],
    "hedtsk_pavlovian_fear_conditioning": [r"fear (conditioning|acquisition|learning)", r"(classical|pavlovian|aversive|threat) conditioning"],
    "hedtsk_reversal_learning": [r"reversal learning"],
    "hedtsk_task_switching": [r"task[- ]switch", r"set[- ]shift", r"switch(ing)? (task|paradigm)"],
    "hedtsk_visual_search": [r"visual search"],
    "hedtsk_attention_network": [r"attention network test"],
    "hedtsk_posner_spatial_cueing": [r"posner", r"spatial cue?ing", r"(endogenous|exogenous) (cue|orienting)"],
    "hedtsk_sternberg_item_recognition": [r"sternberg"],
    "hedtsk_delayed_match_to_sample": [r"delayed[- ]match", r"delayed (recognition|response) (task|paradigm)"],
    "hedtsk_old_new_recognition_memory": [r"old[/ -]new (decision|judg|discrimination|task|recognition)", r"recognition (test|judg)",r"(old[/-]new|yes[/-]no)? ?recognition (memory|task|test)", r"item recognition"],
    "hedtsk_paired_associates_learning": [r"paired[- ]associat", r"face[- ]name (association|pair|memory)", r"associative (memory|learning) (task|paradigm)"],
    "hedtsk_autobiographical_memory": [r"autobiographical"],
    "hedtsk_ultimatum_game": [r"ultimatum"],
    "hedtsk_trust_game": [r"trust game"],
    "hedtsk_dictator_game": [r"dictator game"],
    "hedtsk_prisoners_dilemma": [r"prisoner'?s'? dilemma"],
    "hedtsk_tower_of_london": [r"tower of (london|hanoi)"],
    "hedtsk_serial_reaction_time": [r"serial reaction time"],
    "hedtsk_motor_sequence_learning": [r"(motor )?sequence learning"],
    "hedtsk_anti_saccade": [r"anti[- ]?saccade", r"pro[- ]?saccade"],
    "hedtsk_simon": [r"\bsimon (task|effect|paradigm)", r"hearts (and|&) flowers", r"spatial compatibility"],
    "hedtsk_dot_probe": [r"dot[- ]probe"],
    "hedtsk_implicit_association": [r"implicit association", r"\biat\b"],
    "hedtsk_continuous_performance": [r"continuous performance", r"\bax-?cpt\b"],
    "hedtsk_psychomotor_vigilance": [r"psychomotor vigilance", r"\bpvt\b"],
    "hedtsk_trail_making": [r"trail[- ]making"],
    "hedtsk_digit_span": [r"digit span"],
    "hedtsk_digit_symbol_substitution": [r"digit[- ]symbol"],
    "hedtsk_mismatch_negativity": [r"mismatch negativity", r"\bmmn\b", r"auditory deviance"],
    "hedtsk_false_belief": [r"false[- ]belief"],
    "hedtsk_reading_the_mind_in_the_eyes": [r"(reading the )?mind in the eyes", r"\brmet\b"],
    "hedtsk_social_incentive_delay": [r"social incentive delay"],
    "hedtsk_probabilistic_selection": [r"probabilistic selection"],
    "hedtsk_probabilistic_classification_learning": [r"weather prediction", r"probabilistic (classification|category) learning"],
    "hedtsk_multi_armed_bandit": [r"bandit"],
    "hedtsk_two_stage_decision": [r"two[- ]step|two[- ]stage (decision|task|markov)"],
    "hedtsk_virtual_morris_water_maze": [r"water maze"],
    "hedtsk_sentence_comprehension": [r"sentence (comprehension|processing|judg)", r"(syntactic|grammaticality) (judg|processing)"],
    "hedtsk_semantic_priming": [r"semantic priming"],
    "hedtsk_directed_forgetting": [r"directed forgetting"],
    "hedtsk_think_no_think": [r"think[/ -]no[- ]think"],
    "hedtsk_free_recall": [r"free recall"],
    "hedtsk_source_memory": [r"source memory"],
    "hedtsk_prospective_memory": [r"prospective memory"],
    "hedtsk_heartbeat_detection": [r"heart ?beat (detection|perception|counting|discrimination)", r"interocepti"],
    "hedtsk_effort_based_decision_making": [r"effort[- ]based|effort expenditure|\beefrt\b"],
    "hedtsk_rest": [r"rest(ing)?[- ]?state", r"\brs[- ]?fmri\b", r"\bresting\b", r"\brest\b"],
    "hedtsk_fixate": [r"fixation (task|block|baseline)"],
    "hedtsk_visual_masking": [r"(backward|forward|visual|metacontrast|pattern)[- ]masking", r"masked (target|stimul)"],
    "hedtsk_rapid_serial_visual_presentation": [r"\brsvp\b", r"rapid serial visual", r"attentional blink"],
    "hedtsk_random_dot_kinematogram": [r"random[- ]dot (motion|kinematogram)", r"motion coherence", r"\brdk\b"],
    "hedtsk_change_detection": [r"change detection"],
    "hedtsk_operation_span": [r"operation span|ospan|complex span"],
    "hedtsk_corsi_block_tapping": [r"corsi"],
    "hedtsk_ravens_progressive_matrices": [r"raven"],
    "hedtsk_biological_motion_perception": [r"biological motion|point[- ]light"],
    "hedtsk_instrumental_conditioning": [r"instrumental (learning|conditioning)", r"reinforcement learning (task|paradigm)", r"probabilistic (reward )?learning"],
}

# Tiers: a specific procedure beats a stimulus/presentation class, which beats a generic fallback.
GENERIC_IDS = {"tct_working_memory_generic", "tct_inhibition_generic", "tct_interference_generic", "tct_reward_feedback",
               "tct_motor_execution", "tct_episodic_encoding", "tct_reading", "tct_speech_production"}
STIMULUS_IDS = {"hedtsk_affective_picture_viewing", "hedtsk_face_processing", "tct_pain_stimulation",
                "tct_auditory_stimulation", "tct_sensory_stimulation", "tct_naturalistic_viewing", "tct_checkerboard"}
HEAD_FINAL = 1000  # prefer the rightmost (head) paradigm term in compounds: "masked priming lexical decision"
def _tier(k):
    return 1 if k in GENERIC_IDS else (2 if k in STIMULUS_IDS else 3)

_PRIORITY = {"hed_name": 5, "hed_alias": 4, "hed_variation": 3, "atlas": 2, "regex": 2, "ext": 2}


def _norm(s):
    s = (s or "").lower().replace("–", "-").replace("’", "'")
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\b(tasks?|paradigms?|tests?|games?|experiments?|procedures?)\b", " ", s)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+'/ -]", " ", s)).strip()


class Lexicon:
    def __init__(self):
        self.tasks = {t["hedtsk_id"]: t for t in json.load(open(os.path.join(HED, "task_details.json")))}
        self.names = {k: v["canonical_name"] for k, v in self.tasks.items()}
        self.family = {k: [f["family_id"] for f in v.get("families", []) if f.get("role") == "primary"] or
                       [f["family_id"] for f in v.get("families", [])][:1] for k, v in self.tasks.items()}
        for k, v in BRAINMAP_EXT.items():
            EXTENSIONS.setdefault(k, v)
        for k, (name, fam, _) in EXTENSIONS.items():
            self.names[k] = name; self.family[k] = [fam]
        self.entries = []  # (normalized phrase, task_id, variation_id, source)
        for k, t in self.tasks.items():
            self.entries.append((_norm(t["canonical_name"]), k, None, "hed_name"))
            for a in t.get("aliases", []):
                self.entries.append((_norm(a), k, None, "hed_alias"))
            for v in t.get("variations", []):
                self.entries.append((_norm(v["name"]), k, v["variation_id"], "hed_variation"))
        with open(os.path.join(HED, "mappings", "atlas_task_to_hed.tsv")) as f:
            for r in csv.DictReader(f, delimiter="\t"):
                if r["match_type"] in ("exact", "close") and r["hedtsk_id"] and r["atlas_name"]:
                    self.entries.append((_norm(r["atlas_name"]), r["hedtsk_id"], r.get("hed_variation_id") or None, "atlas"))
        for nm, k in BRAINMAP_NAMES:
            if k in self.names and nm.lower() not in ("gambling",):
                self.entries.append((_norm(nm), k, None, "atlas"))
        self.entries = [e for e in self.entries if len(e[0]) >= 3]
        # every pattern must start at a word boundary ("motion processing" must not match inside "emotion processing")
        wb = lambda p: re.compile(r"(?<![A-Za-z])(?:" + p + r")", re.I)
        self.patterns = [(wb(p), k, "regex") for k, ps in HED_PATTERNS.items() if k in self.tasks for p in ps]
        self.patterns += [(wb(p), k, "ext") for k, (_, _, ps) in EXTENSIONS.items() for p in ps]
        self.bad_patterns = [k for k in HED_PATTERNS if k not in self.tasks]

    def match(self, *texts):
        """Return best (task_id, variation_id, source, matched_text) or None. texts in priority order."""
        for text in texts:
            if not text:
                continue
            n = " " + _norm(text) + " "
            best = None
            for phrase, k, var, src in self.entries:
                if f" {phrase} " in n:
                    end = n.rfind(f" {phrase} ") + len(phrase)
                    score = (_tier(k), HEAD_FINAL * end + len(phrase), _PRIORITY[src])
                    if best is None or score > best[0]:
                        best = (score, k, var, src, phrase)
            bare = re.sub(r"\s*\([^)]*\)", " ", text)
            for pat, k, src in self.patterns:
                m = pat.search(bare)
                if not m and best is None:
                    m = pat.search(text)
                    if m:
                        bare_end = None
                if m and pat.search(bare) is None and best is not None:
                    m = None
                if m:
                    end = len(_norm((bare if pat.search(bare) else text)[:m.end()]))
                    score = (_tier(k), HEAD_FINAL * end + len(m.group(0)), _PRIORITY[src])
                    if best is None or score > best[0]:
                        best = (score, k, None, src, m.group(0))
            if best:
                return best[1:]
        return None
