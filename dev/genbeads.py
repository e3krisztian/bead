#!/usr/bin/env python
# coding: utf-8
"""
A script to generate a set of bead archives with specified properties.
"""

import argparse
import random
import sys
import zipfile
from dataclasses import dataclass, field
from bead.infra.fs import Path
from typing import Dict, List

from bead.workspace import Workspace
from bead import layouts
from bead.infra import timestamp
from bead.infra import fs
from bead.ziparchive import ZipArchive

# Data Dictionaries

ELEMENTS = {
    "Hydrogen": ["Helium"], "Helium": ["Hydrogen", "Lithium"], "Lithium": ["Helium", "Beryllium"],
    "Beryllium": ["Lithium", "Boron"], "Boron": ["Beryllium", "Carbon"], "Carbon": ["Boron", "Nitrogen"],
    "Nitrogen": ["Carbon", "Oxygen"], "Oxygen": ["Nitrogen", "Fluorine"], "Fluorine": ["Oxygen", "Neon"],
    "Neon": ["Fluorine", "Sodium"], "Sodium": ["Neon", "Magnesium"], "Magnesium": ["Sodium", "Aluminum"],
    "Aluminum": ["Magnesium", "Silicon"], "Silicon": ["Aluminum", "Phosphorus"],
    "Phosphorus": ["Silicon", "Sulfur"], "Sulfur": ["Phosphorus", "Chlorine"],
    "Chlorine": ["Sulfur", "Argon"], "Argon": ["Chlorine", "Potassium"],
    "Potassium": ["Argon", "Calcium"], "Calcium": ["Potassium", "Scandium"],
    "Scandium": ["Calcium", "Titanium"], "Titanium": ["Scandium", "Vanadium"],
    "Vanadium": ["Titanium", "Chromium"], "Chromium": ["Vanadium", "Manganese"],
    "Manganese": ["Chromium", "Iron"], "Iron": ["Manganese", "Cobalt"], "Cobalt": ["Iron", "Nickel"],
    "Nickel": ["Cobalt", "Copper"], "Copper": ["Nickel", "Zinc"], "Zinc": ["Copper", "Gallium"],
    "Gallium": ["Zinc", "Germanium"], "Germanium": ["Gallium", "Arsenic"],
    "Arsenic": ["Germanium", "Selenium"], "Selenium": ["Arsenic", "Bromine"],
    "Bromine": ["Selenium", "Krypton"], "Krypton": ["Bromine", "Rubidium"],
    "Rubidium": ["Krypton", "Strontium"], "Strontium": ["Rubidium", "Yttrium"],
    "Yttrium": ["Strontium", "Zirconium"], "Zirconium": ["Yttrium", "Niobium"],
    "Niobium": ["Zirconium", "Molybdenum"], "Molybdenum": ["Niobium", "Technetium"],
    "Technetium": ["Molybdenum", "Ruthenium"], "Ruthenium": ["Technetium", "Rhodium"],
    "Rhodium": ["Ruthenium", "Palladium"], "Palladium": ["Rhodium", "Silver"],
    "Silver": ["Palladium", "Cadmium"], "Cadmium": ["Silver", "Indium"], "Indium": ["Cadmium", "Tin"],
    "Tin": ["Indium", "Antimony"], "Antimony": ["Tin", "Tellurium"],
    "Tellurium": ["Antimony", "Iodine"], "Iodine": ["Tellurium", "Xenon"], "Xenon": ["Iodine", "Caesium"],
    "Caesium": ["Xenon", "Barium"], "Barium": ["Caesium", "Lanthanum"],
    "Lanthanum": ["Barium", "Cerium"], "Cerium": ["Lanthanum", "Praseodymium"],
    "Praseodymium": ["Cerium", "Neodymium"], "Neodymium": ["Praseodymium", "Promethium"],
    "Promethium": ["Neodymium", "Samarium"], "Samarium": ["Promethium", "Europium"],
    "Europium": ["Samarium", "Gadolinium"], "Gadolinium": ["Europium", "Terbium"],
    "Terbium": ["Gadolinium", "Dysprosium"], "Dysprosium": ["Terbium", "Holmium"],
    "Holmium": ["Dysprosium", "Erbium"], "Erbium": ["Holmium", "Thulium"],
    "Thulium": ["Erbium", "Ytterbium"], "Ytterbium": ["Thulium", "Lutetium"],
    "Lutetium": ["Ytterbium", "Hafnium"], "Hafnium": ["Lutetium", "Tantalum"],
    "Tantalum": ["Hafnium", "Tungsten"], "Tungsten": ["Tantalum", "Rhenium"],
    "Rhenium": ["Tungsten", "Osmium"], "Osmium": ["Rhenium", "Iridium"], "Iridium": ["Osmium", "Platinum"],
    "Platinum": ["Iridium", "Gold"], "Gold": ["Platinum", "Mercury"], "Mercury": ["Gold", "Thallium"],
    "Thallium": ["Mercury", "Lead"], "Lead": ["Thallium", "Bismuth"], "Bismuth": ["Lead", "Polonium"],
    "Polonium": ["Bismuth", "Astatine"], "Astatine": ["Polonium", "Radon"], "Radon": ["Astatine", "Francium"],
    "Francium": ["Radon", "Radium"], "Radium": ["Francium", "Actinium"],
    "Actinium": ["Radium", "Thorium"], "Thorium": ["Actinium", "Protactinium"],
    "Protactinium": ["Thorium", "Uranium"], "Uranium": ["Protactinium", "Neptunium"],
    "Neptunium": ["Uranium", "Plutonium"], "Plutonium": ["Neptunium", "Americium"],
    "Americium": ["Plutonium", "Curium"], "Curium": ["Americium", "Berkelium"],
    "Berkelium": ["Curium", "Californium"], "Californium": ["Berkelium", "Einsteinium"],
    "Einsteinium": ["Californium", "Fermium"], "Fermium": ["Einsteinium", "Mendelevium"],
    "Mendelevium": ["Fermium", "Nobelium"], "Nobelium": ["Mendelevium", "Lawrencium"],
    "Lawrencium": ["Nobelium", "Rutherfordium"], "Rutherfordium": ["Lawrencium", "Dubnium"],
    "Dubnium": ["Rutherfordium", "Seaborgium"], "Seaborgium": ["Dubnium", "Bohrium"],
    "Bohrium": ["Seaborgium", "Hassium"], "Hassium": ["Bohrium", "Meitnerium"],
    "Meitnerium": ["Hassium", "Darmstadtium"], "Darmstadtium": ["Meitnerium", "Roentgenium"],
    "Roentgenium": ["Darmstadtium", "Copernicium"], "Copernicium": ["Roentgenium", "Nihonium"],
    "Nihonium": ["Copernicium", "Flerovium"], "Flerovium": ["Nihonium", "Moscovium"],
    "Moscovium": ["Flerovium", "Livermorium"], "Livermorium": ["Moscovium", "Tennessine"],
    "Tennessine": ["Livermorium", "Oganesson"], "Oganesson": ["Tennessine"],
}

SOLAR_SYSTEM = {
    "Sun": ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune",
            "Ceres", "Pluto", "Haumea", "Makemake", "Eris", "Quaoar", "Sedna", "Orcus", "Gonggong",
            "Vesta", "Pallas", "Hygiea", "Juno", "Davida", "Interamnia", "Europa_asteroid",
            "Halley", "Encke", "Hale_Bopp", "Churyumov_Gerasimenko", "Tempel1", "Wild2"],
    "Mercury": [], "Venus": [],
    "Earth": ["Moon"], "Moon": [],
    "Mars": ["Phobos", "Deimos"], "Phobos": [], "Deimos": [],
    "Ceres": [],
    "Jupiter": ["Io", "Europa", "Ganymede", "Callisto",
                "Amalthea", "Thebe", "Adrastea", "Metis",
                "Himalia", "Elara", "Pasiphae", "Sinope", "Lysithea",
                "Carme", "Ananke", "Leda",
                "Callirrhoe", "Themisto", "Megaclite", "Taygete", "Chaldene",
                "Harpalyke", "Kalyke", "Iocaste", "Erinome", "Isonoe", "Praxidike"],
    "Io": [], "Europa": [], "Ganymede": [], "Callisto": [],
    "Amalthea": [], "Thebe": [], "Adrastea": [], "Metis": [],
    "Himalia": [], "Lysithea": [], "Elara": [], "Pasiphae": [], "Sinope": [],
    "Carme": [], "Ananke": [], "Leda": [],
    "Callirrhoe": [], "Themisto": [], "Megaclite": [], "Taygete": [], "Chaldene": [],
    "Harpalyke": [], "Kalyke": [], "Iocaste": [], "Erinome": [], "Isonoe": [], "Praxidike": [],
    "Saturn": ["Mimas", "Enceladus", "Tethys", "Dione", "Rhea", "Titan", "Iapetus", "Hyperion", "Phoebe",
               "Janus", "Epimetheus", "Helene", "Telesto", "Calypso", "Atlas", "Prometheus", "Pandora", "Pan", "Daphnis",
               "Methone", "Pallene", "Polydeuces", "Anthe", "Aegaeon",
               "Kiviuq", "Ijiraq", "Paaliaq", "Skathi", "Albiorix", "Bebhionn", "Erriapus",
               "Siarnaq", "Tarqeq", "Narvi", "Aegir", "Bebhionn", "Bergelmir", "Bestla",
               "Farbauti", "Fenrir", "Fornjot", "Hati", "Hyrrokkin", "Kari", "Loge", "Skoll", "Surtur"],
    "Mimas": [], "Enceladus": [], "Tethys": [], "Dione": [], "Rhea": [], "Titan": [],
    "Iapetus": [], "Hyperion": [], "Phoebe": [], "Janus": [], "Epimetheus": [],
    "Helene": [], "Telesto": [], "Calypso": [], "Atlas": [], "Prometheus": [], "Pandora": [],
    "Pan": [], "Daphnis": [],
    "Methone": [], "Pallene": [], "Polydeuces": [], "Anthe": [], "Aegaeon": [],
    "Kiviuq": [], "Ijiraq": [], "Paaliaq": [], "Skathi": [], "Albiorix": [],
    "Siarnaq": [], "Tarqeq": [], "Narvi": [], "Aegir": [], "Bergelmir": [], "Bestla": [],
    "Farbauti": [], "Fenrir": [], "Fornjot": [], "Hati": [], "Hyrrokkin": [],
    "Kari": [], "Loge": [], "Skoll": [], "Surtur": [], "Bebhionn": [], "Erriapus": [],
    "Uranus": ["Ariel", "Umbriel", "Titania", "Oberon", "Miranda",
               "Cordelia", "Ophelia", "Bianca", "Cressida", "Desdemona",
               "Juliet", "Portia", "Rosalind", "Belinda", "Puck",
               "Caliban", "Sycorax", "Prospero", "Setebos", "Stephano",
               "Trinculo", "Francisco", "Margaret", "Ferdinand", "Perdita",
               "Mab", "Cupid"],
    "Ariel": [], "Umbriel": [], "Titania": [], "Oberon": [], "Miranda": [],
    "Cordelia": [], "Ophelia": [], "Bianca": [], "Cressida": [], "Desdemona": [],
    "Juliet": [], "Portia": [], "Rosalind": [], "Belinda": [], "Puck": [],
    "Caliban": [], "Sycorax": [], "Prospero": [], "Setebos": [], "Stephano": [],
    "Trinculo": [], "Francisco": [], "Margaret": [], "Ferdinand": [], "Perdita": [],
    "Mab": [], "Cupid": [],
    "Neptune": ["Triton", "Nereid", "Proteus", "Larissa", "Galatea", "Despina", "Thalassa", "Naiad",
                "Halimede", "Sao", "Laomedeia", "Neso", "Psamathe"],
    "Triton": [], "Nereid": [], "Proteus": [], "Larissa": [], "Galatea": [],
    "Despina": [], "Thalassa": [], "Naiad": [],
    "Halimede": [], "Sao": [], "Laomedeia": [], "Neso": [], "Psamathe": [],
    "Pluto": ["Charon", "Nix", "Hydra", "Styx", "Kerberos"],
    "Charon": [], "Nix": [], "Hydra": [], "Styx": [], "Kerberos": [],
    "Haumea": ["Namaka", "Hiʻiaka"], "Namaka": [], "Hiʻiaka": [],
    "Makemake": ["MK2"], "MK2": [],
    "Eris": ["Dysnomia"], "Dysnomia": [],
    "Quaoar": ["Weywot"], "Weywot": [],
    "Orcus": ["Vanth"], "Vanth": [],
    "Gonggong": ["Xiangliu"], "Xiangliu": [],
    "Sedna": [],
    # Asteroids
    "Vesta": [], "Pallas": [], "Hygiea": [], "Juno": [],
    "Davida": [], "Interamnia": [], "Europa_asteroid": [],
    # Comets
    "Halley": [], "Encke": [], "Hale_Bopp": [],
    "Churyumov_Gerasimenko": [], "Tempel1": [], "Wild2": [],
}

GREEK_MYTHOLOGY = {
    # Primordials
    "Chaos": ["Gaia", "Tartarus", "Eros", "Erebus", "Nyx"],
    "Erebus": ["Aether", "Hemera"],
    "Nyx": ["Hypnos", "Thanatos", "Eris", "Nemesis", "Moros", "Ker", "Momus", "Oizys", "Apate", "Philotes", "Geras"],
    "Gaia": ["Uranus", "Pontus", "Ourea", "Erinyes"],
    "Erinyes": ["Alecto", "Megaera", "Tisiphone"],
    "Alecto": [], "Megaera": [], "Tisiphone": [],
    "Pontus": ["Nereus", "Thaumas", "Phorcys", "Ceto", "Eurybia"],
    "Tartarus": ["Typhon"],
    # Sky and Titans
    "Uranus": ["Titans", "Cyclopes", "Hecatoncheires"],
    "Titans": ["Oceanus", "Tethys", "Hyperion", "Theia", "Coeus", "Phoebe", "Cronus", "Rhea", "Mnemosyne", "Themis", "Crius", "Iapetus"],
    "Oceanus": ["Oceanids", "Potamoi"],
    "Tethys": ["Oceanids", "Potamoi"],
    "Hyperion": ["Helios", "Selene", "Eos"],
    "Theia": ["Helios", "Selene", "Eos"],
    "Coeus": ["Leto", "Asteria"],
    "Phoebe": ["Leto", "Asteria"],
    "Mnemosyne": ["Muses"],
    "Themis": ["Horae", "Moirai"],
    "Crius": ["Astraeus", "Pallas", "Perses"],
    "Astraeus": ["Anemoi", "Astraea"],
    # Olympians and their children
    "Cronus": ["Hestia", "Hades", "Demeter", "Poseidon", "Hera", "Zeus"],
    "Rhea": ["Hestia", "Hades", "Demeter", "Poseidon", "Hera", "Zeus"],
    "Zeus": ["Athena", "Apollo", "Artemis", "Ares", "Hephaestus", "Hebe", "Eileithyia", "Persephone", "Dionysus", "Hermes", "Heracles", "Perseus", "Minos", "Rhadamanthus", "Sarpedon", "Castor", "Pollux", "Helen", "Tantalus"],
    "Hera": ["Ares", "Hephaestus", "Hebe", "Eileithyia"],
    "Poseidon": ["Triton", "Polyphemus", "Theseus", "Orion", "Bellerophon", "Chrysaor", "Pegasus"],
    "Hades": ["Zagreus", "Macaria", "Melinoe"],
    "Demeter": ["Persephone", "Arion", "Plutus"],
    "Apollo": ["Asclepius", "Orpheus", "Aristaeus", "Ion", "Troilus"],
    "Athena": ["Erichthonius"],
    "Ares": ["Phobos", "Deimos", "Eros", "Anteros", "Harmonia", "Cycnus"],
    "Aphrodite": ["Eros", "Anteros", "Hermaphroditus", "Aeneas", "Harmonia"],
    "Hermes": ["Pan", "Abderus", "Autolycus", "Hermaphroditus"],
    "Dionysus": ["Priapus", "Comus"],
    "Hephaestus": [],
    "Hestia": [],
    "Artemis": [],
    "Leto": ["Apollo", "Artemis"],
    "Persephone": ["Melinoe", "Zagreus"],
    # Titans' offspring
    "Iapetus": ["Atlas", "Prometheus", "Epimetheus", "Menoetius"],
    "Prometheus": ["Deucalion"],
    "Epimetheus": ["Pyrrha"],
    "Atlas": ["Hesperides", "Hyades", "Pleiades", "Calypso", "Maia"],
    "Maia": ["Hermes"],
    "Nereus": ["Nereids"],
    "Phorcys": ["Graeae", "Gorgons", "Echidna"],
    "Ceto": ["Graeae", "Gorgons", "Echidna"],
    "Echidna": ["Typhon", "Cerberus", "Hydra", "Chimera", "Sphinx", "Nemean_Lion", "Scylla", "Ladon"],
    "Typhon": ["Cerberus", "Hydra", "Chimera", "Sphinx", "Nemean_Lion", "Orthrus"],
    # Heroes
    "Heracles": ["Telephus", "Hyllus"],
    "Perseus": ["Perses", "Electryon"],
    "Theseus": ["Hippolytus", "Acamas", "Demophon"],
    "Asclepius": ["Hygieia", "Panacea", "Iaso", "Aceso", "Machaon", "Podalirius"],
    "Orpheus": [],
    "Deucalion": ["Hellen"],
    "Hellen": ["Dorus", "Xuthus", "Aeolus"],
    # Other notable figures
    "Eos": ["Memnon", "Emathion", "Zephyrus", "Boreas", "Notus"],
    "Helios": ["Phaethon", "Circe", "Aeetes", "Pasiphae"],
    "Selene": ["Endymion"],
    "Circe": ["Telegonus"],
    "Aeetes": ["Medea", "Absyrtus"],
    "Medea": ["Medus"],
    "Anemoi": [],
    "Muses": [],
    "Horae": [],
    "Moirai": [],
    "Nereids": [],
    "Oceanids": [],
    "Potamoi": [],
    "Hesperides": [],
    "Hyades": [],
    "Pleiades": [],
    "Graeae": [],
    "Gorgons": [],
    "Hypnos": ["Morpheus", "Phobetor", "Phantasos"],
    "Morpheus": [],
    "Eris": ["Ate", "Dysnomia"],
    "Nemesis": [],
    "Hecate": [],
    "Calypso": [],
    "Pan": [],
    "Triton": [],
    "Hygieia": [], "Panacea": [],
    "Cerberus": [], "Hydra": [], "Chimera": [], "Sphinx": [], "Nemean_Lion": [], "Scylla": [], "Ladon": [], "Orthrus": [],
    "Priapus": [], "Comus": [],
    "Polyphemus": [], "Orion": [], "Bellerophon": [], "Chrysaor": [], "Pegasus": [],
    "Phaethon": [], "Telegonus": [], "Memnon": [],
    # Leaves: children of primordials/Titans
    "Eros": [], "Aether": [], "Hemera": [],
    "Thanatos": [], "Moros": [], "Ker": [], "Momus": [], "Oizys": [], "Apate": [], "Philotes": [], "Geras": [],
    "Ourea": [], "Thaumas": [], "Eurybia": [],
    "Cyclopes": [], "Hecatoncheires": [],
    "Asteria": [], "Astraea": [],
    # Leaves: Olympian children
    "Hebe": [], "Eileithyia": [],
    "Minos": [], "Rhadamanthus": [], "Sarpedon": [],
    "Castor": [], "Pollux": [], "Helen": [], "Tantalus": [],
    "Zagreus": [], "Macaria": [], "Melinoe": [],
    "Arion": [], "Plutus": [],
    "Aristaeus": [], "Ion": [], "Troilus": [],
    "Erichthonius": [],
    "Harmonia": [], "Cycnus": [],
    "Anteros": [], "Hermaphroditus": [], "Aeneas": [],
    "Abderus": [], "Autolycus": [],
    # Leaves: Titan offspring
    "Menoetius": [],
    "Pyrrha": [],
    # Leaves: Heroes' children
    "Telephus": [], "Hyllus": [],
    "Perses": [], "Electryon": [],
    "Hippolytus": [], "Acamas": [], "Demophon": [],
    "Iaso": [], "Aceso": [], "Machaon": [], "Podalirius": [],
    # Leaves: Hellen's children
    "Dorus": [], "Xuthus": [], "Aeolus": [],
    # Leaves: Eos / wind gods
    "Emathion": [], "Zephyrus": [], "Boreas": [], "Notus": [],
    # Leaves: Helios lineage
    "Pasiphae": [], "Endymion": [], "Absyrtus": [], "Medus": [],
    # Leaves: Hypnos children
    "Phobetor": [], "Phantasos": [],
    # Leaves: Eris children
    "Ate": [], "Dysnomia": [],
    # Leaves: also appear in other domains
    "Phobos": [], "Deimos": [], "Pallas": [],
}

GEOGRAPHY = {
    # Continents / regions
    "Africa": ["Nigeria", "Egypt", "South_Africa", "Kenya", "Morocco", "Ethiopia", "Ghana", "Algeria", "Uganda", "Tanzania",
               "Senegal", "Ivory_Coast", "Cameroon", "Mozambique", "Madagascar", "Angola", "Zambia", "Zimbabwe", "Rwanda", "Sudan"],
    "Asia": ["China", "India", "Japan", "South_Korea", "Thailand", "Vietnam", "Indonesia", "Malaysia", "Philippines", "Singapore",
             "Pakistan", "Bangladesh", "Myanmar", "Cambodia", "Sri_Lanka", "Nepal", "Uzbekistan", "Kazakhstan", "Azerbaijan", "Georgia_country"],
    "Middle_East": ["Turkey", "Iran", "Saudi_Arabia", "Iraq", "Israel", "Jordan", "Lebanon", "Syria", "UAE", "Kuwait",
                    "Qatar", "Bahrain", "Oman", "Yemen", "Afghanistan"],
    "Europe": ["Germany", "France", "United_Kingdom", "Italy", "Spain", "Poland", "Netherlands", "Belgium", "Greece", "Portugal",
               "Sweden", "Norway", "Denmark", "Finland", "Switzerland", "Austria", "Czech_Republic", "Hungary", "Romania", "Ukraine",
               "Serbia", "Croatia", "Slovakia", "Bulgaria", "Belarus", "Lithuania", "Latvia", "Estonia", "Slovenia", "Ireland"],
    "North_America": ["United_States", "Canada", "Mexico", "Guatemala", "Cuba", "Jamaica", "Costa_Rica", "Panama", "Honduras", "Nicaragua",
                      "Dominican_Republic", "Haiti", "El_Salvador", "Belize", "Trinidad_and_Tobago"],
    "South_America": ["Brazil", "Argentina", "Chile", "Peru", "Colombia", "Venezuela", "Ecuador", "Uruguay", "Paraguay", "Bolivia",
                      "Guyana", "Suriname"],
    "Oceania": ["Australia_country", "New_Zealand", "Papua_New_Guinea", "Fiji", "Solomon_Islands", "Vanuatu", "Samoa", "Kiribati", "Tonga"],
    "Antarctica": [],

    # Africa
    "Nigeria": ["Lagos", "Abuja", "Kano", "Ibadan", "Port_Harcourt"],
    "Egypt": ["Cairo", "Alexandria", "Giza", "Luxor", "Aswan"],
    "South_Africa": ["Cape_Town", "Johannesburg", "Durban", "Pretoria", "Port_Elizabeth"],
    "Kenya": ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"],
    "Morocco": ["Casablanca", "Rabat", "Marrakech", "Fez", "Tangier"],
    "Ethiopia": ["Addis_Ababa", "Dire_Dawa", "Mekelle", "Gondar", "Hawassa"],
    "Ghana": ["Accra", "Kumasi", "Tamale", "Sekondi", "Cape_Coast"],
    "Algeria": ["Algiers", "Oran", "Constantine", "Annaba", "Blida"],
    "Uganda": ["Kampala", "Gulu", "Lira", "Mbarara", "Jinja"],
    "Tanzania": ["Dar_es_Salaam", "Dodoma", "Mwanza", "Arusha", "Mbeya"],
    "Senegal": ["Dakar", "Touba", "Thies", "Kaolack", "Ziguinchor"],
    "Ivory_Coast": ["Abidjan", "Bouake", "Daloa", "Yamoussoukro", "Korhogo"],
    "Cameroon": ["Douala", "Yaounde", "Garoua", "Kousseris", "Bamenda"],
    "Mozambique": ["Maputo", "Matola", "Nampula", "Beira", "Chimoio"],
    "Madagascar": ["Antananarivo", "Toamasina", "Antsirabe", "Fianarantsoa", "Mahajanga"],
    "Angola": ["Luanda", "Huambo", "Lobito", "Benguela", "Namibe"],
    "Zambia": ["Lusaka", "Kitwe", "Ndola", "Kabwe", "Chingola"],
    "Zimbabwe": ["Harare", "Bulawayo", "Chitungwiza", "Mutare", "Gweru"],
    "Rwanda": ["Kigali", "Butare", "Gitarama", "Musanze", "Byumba"],
    "Sudan": ["Khartoum", "Omdurman", "Port_Sudan", "Kassala", "El_Obeid"],

    # Asia (East/Southeast/South)
    "China": ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu", "Hangzhou", "Wuhan", "Xian", "Nanjing", "Tianjin"],
    "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Surat"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Kobe", "Nagoya", "Sapporo", "Fukuoka", "Hiroshima", "Sendai"],
    "South_Korea": ["Seoul", "Busan", "Incheon", "Daegu", "Daejeon", "Gwangju", "Ulsan", "Suwon"],
    "Thailand": ["Bangkok", "Chiang_Mai", "Phuket", "Pattaya", "Hat_Yai", "Nakhon_Ratchasima"],
    "Vietnam": ["Ho_Chi_Minh_City", "Hanoi", "Da_Nang", "Hai_Phong", "Can_Tho", "Hue"],
    "Indonesia": ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Palembang"],
    "Malaysia": ["Kuala_Lumpur", "George_Town", "Ipoh", "Shah_Alam", "Petaling_Jaya", "Johor_Bahru"],
    "Philippines": ["Manila", "Quezon_City", "Davao", "Cebu_City", "Zamboanga", "Antipolo"],
    "Singapore": ["Singapore_City"],
    "Pakistan": ["Karachi", "Lahore", "Faisalabad", "Rawalpindi", "Multan", "Islamabad", "Peshawar", "Quetta"],
    "Bangladesh": ["Dhaka", "Chittagong", "Sylhet", "Rajshahi", "Khulna", "Comilla"],
    "Myanmar": ["Naypyidaw", "Yangon", "Mandalay", "Mawlamyine", "Bago"],
    "Cambodia": ["Phnom_Penh", "Siem_Reap", "Battambang", "Sihanoukville", "Kampong_Cham"],
    "Sri_Lanka": ["Colombo", "Kandy", "Galle", "Jaffna", "Negombo"],
    "Nepal": ["Kathmandu", "Pokhara", "Lalitpur", "Bharatpur", "Birgunj"],
    "Uzbekistan": ["Tashkent", "Samarkand", "Namangan", "Andijan", "Nukus"],
    "Kazakhstan": ["Almaty", "Astana", "Shymkent", "Karaganda", "Aktobe"],
    "Azerbaijan": ["Baku", "Ganja", "Sumqayit", "Mingachevir", "Nakhchivan"],
    "Georgia_country": ["Tbilisi", "Kutaisi", "Batumi", "Rustavi", "Gori"],

    # Middle East
    "Turkey": ["Istanbul", "Ankara", "Izmir", "Bursa", "Adana", "Gaziantep", "Konya", "Antalya"],
    "Iran": ["Tehran", "Mashhad", "Isfahan", "Tabriz", "Shiraz", "Ahvaz", "Qom", "Karaj"],
    "Saudi_Arabia": ["Riyadh", "Jeddah", "Mecca", "Medina", "Dammam", "Taif", "Tabuk"],
    "Iraq": ["Baghdad", "Basra", "Mosul", "Erbil", "Kirkuk", "Najaf", "Karbala"],
    "Israel": ["Jerusalem", "Tel_Aviv", "Haifa", "Rishon_LeZion", "Petah_Tikva", "Ashdod", "Beer_Sheva"],
    "Jordan": ["Amman", "Zarqa", "Irbid", "Russeifa", "Wadi_as_Seer"],
    "Lebanon": ["Beirut", "Tripoli_Lebanon", "Sidon", "Tyre", "Jounieh"],
    "UAE": ["Dubai", "Abu_Dhabi", "Sharjah", "Al_Ain", "Ajman"],
    "Qatar": ["Doha", "Al_Rayyan", "Al_Wakrah", "Al_Khor"],
    "Kuwait": ["Kuwait_City", "Hawalli", "Salmiya", "Ahmadi"],
    "Oman": ["Muscat", "Salalah", "Sohar", "Nizwa", "Sur"],
    "Afghanistan": ["Kabul", "Kandahar", "Herat", "Mazar_e_Sharif", "Jalalabad"],
    "Syria": ["Damascus", "Aleppo", "Homs", "Latakia", "Hama"],
    "Yemen": ["Sanaa", "Aden", "Taiz", "Al_Hudaydah", "Ibb"],
    "Bahrain": ["Manama", "Riffa", "Muharraq", "Hamad_Town"],

    # Europe (existing + new)
    "Germany": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt", "Stuttgart", "Dusseldorf", "Dortmund", "Essen", "Leipzig"],
    "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"],
    "United_Kingdom": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Leeds", "Sheffield", "Edinburgh", "Bristol", "Cardiff"],
    "Italy": ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence", "Bari", "Catania"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Malaga", "Murcia", "Palma", "Las_Palmas", "Bilbao"],
    "Poland": ["Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan", "Gdansk", "Szczecin", "Bydgoszcz", "Lublin", "Katowice"],
    "Netherlands": ["Amsterdam", "Rotterdam", "The_Hague", "Utrecht", "Eindhoven", "Tilburg", "Groningen", "Almere"],
    "Belgium": ["Brussels", "Antwerp", "Ghent", "Charleroi", "Liege", "Bruges", "Namur", "Leuven"],
    "Greece": ["Athens", "Thessaloniki", "Patras", "Heraklion", "Larissa", "Volos", "Rhodes", "Ioannina"],
    "Portugal": ["Lisbon", "Porto", "Vila_Nova_de_Gaia", "Amadora", "Braga", "Funchal", "Coimbra", "Setubal"],
    "Sweden": ["Stockholm", "Gothenburg", "Malmo", "Uppsala", "Vasteras", "Orebro", "Linkoping"],
    "Norway": ["Oslo", "Bergen", "Trondheim", "Stavanger", "Drammen", "Fredrikstad"],
    "Denmark": ["Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg", "Randers"],
    "Finland": ["Helsinki", "Espoo", "Tampere", "Vantaa", "Oulu", "Turku"],
    "Switzerland": ["Zurich", "Geneva", "Basel", "Bern", "Lausanne", "Winterthur"],
    "Austria": ["Vienna", "Graz", "Linz", "Salzburg", "Innsbruck", "Klagenfurt"],
    "Czech_Republic": ["Prague", "Brno", "Ostrava", "Plzen", "Liberec", "Olomouc"],
    "Hungary": ["Budapest", "Debrecen", "Miskolc", "Szeged", "Pecs", "Gyor"],
    "Romania": ["Bucharest", "Cluj_Napoca", "Timisoara", "Iasi", "Constanta", "Craiova"],
    "Ukraine": ["Kyiv", "Kharkiv", "Odessa", "Dnipro", "Donetsk", "Lviv", "Zaporizhzhia"],
    "Serbia": ["Belgrade", "Novi_Sad", "Nis", "Kragujevac", "Subotica"],
    "Croatia": ["Zagreb", "Split", "Rijeka", "Osijek", "Zadar"],
    "Ireland": ["Dublin", "Cork", "Limerick", "Galway", "Waterford"],

    # North America
    "United_States": ["New_York", "Los_Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San_Antonio", "San_Diego", "Dallas", "San_Jose",
                      "Austin", "Jacksonville", "Fort_Worth", "Columbus", "Charlotte", "Indianapolis", "San_Francisco", "Seattle", "Denver", "Nashville"],
    "Canada": ["Toronto", "Montreal", "Vancouver", "Calgary", "Edmonton", "Ottawa", "Winnipeg", "Quebec_City", "Hamilton", "Kitchener"],
    "Mexico": ["Mexico_City", "Guadalajara", "Monterrey", "Puebla", "Tijuana", "Leon", "Juarez", "Torreon", "Queretaro", "Merida"],
    "Guatemala": ["Guatemala_City", "Mixco", "Villa_Nueva", "Petapa", "San_Juan_Sacatepequez"],
    "Cuba": ["Havana", "Santiago_de_Cuba", "Camaguey", "Holguin", "Santa_Clara"],
    "Jamaica": ["Kingston", "Spanish_Town", "Portmore", "Montego_Bay", "May_Pen"],
    "Costa_Rica": ["San_Jose", "Cartago", "Puntarenas", "Limon", "Alajuela"],
    "Panama": ["Panama_City", "San_Miguelito", "Tocumen", "David", "Arraijan"],
    "Honduras": ["Tegucigalpa", "San_Pedro_Sula", "Choloma", "La_Ceiba", "El_Progreso"],
    "Nicaragua": ["Managua", "Leon", "Masaya", "Matagalpa", "Chinandega"],
    "Dominican_Republic": ["Santo_Domingo", "Santiago_DR", "La_Romana", "San_Pedro_de_Macoris", "Puerto_Plata"],
    "Haiti": ["Port_au_Prince", "Cap_Haitien", "Gonaives", "Saint_Marc", "Petionville"],
    "El_Salvador": ["San_Salvador", "Soyapango", "Santa_Ana", "San_Miguel", "Mejicanos"],
    "Trinidad_and_Tobago": ["Port_of_Spain", "San_Fernando", "Chaguanas", "Arima"],

    # South America
    "Brazil": ["Sao_Paulo", "Rio_de_Janeiro", "Brasilia", "Salvador", "Fortaleza", "Belo_Horizonte", "Manaus", "Curitiba", "Recife", "Porto_Alegre"],
    "Argentina": ["Buenos_Aires", "Cordoba", "Rosario", "Mendoza", "Tucuman", "La_Plata", "Mar_del_Plata", "Salta", "Santa_Fe", "San_Juan"],
    "Chile": ["Santiago", "Valparaiso", "Concepcion", "La_Serena", "Antofagasta", "Temuco", "Rancagua", "Talca", "Arica", "Chilian"],
    "Peru": ["Lima", "Arequipa", "Trujillo", "Chiclayo", "Huancayo", "Piura", "Iquitos", "Cusco", "Chimbote", "Tacna"],
    "Colombia": ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena", "Cucuta", "Bucaramanga", "Pereira", "Santa_Marta", "Ibague"],
    "Venezuela": ["Caracas", "Maracaibo", "Valencia", "Barquisimeto", "Maracay", "Ciudad_Guayana", "San_Cristobal", "Maturin", "Ciudad_Bolivar", "Cumana"],
    "Ecuador": ["Quito", "Guayaquil", "Cuenca", "Santo_Domingo_EC", "Machala", "Manta", "Portoviejo", "Ambato", "Riobamba", "Esmeraldas"],
    "Uruguay": ["Montevideo", "Salto", "Paysandu", "Las_Piedras", "Rivera"],
    "Paraguay": ["Asuncion", "Ciudad_del_Este", "San_Lorenzo", "Luque", "Capiata"],
    "Bolivia": ["La_Paz", "Santa_Cruz", "Cochabamba", "Oruro", "Sucre"],
    "Guyana": ["Georgetown", "Linden", "New_Amsterdam", "Anna_Regina"],
    "Suriname": ["Paramaribo", "Lelydorp", "Nieuw_Nickerie", "Moengo"],

    # Oceania
    "Australia_country": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra", "Darwin", "Hobart", "Gold_Coast", "Newcastle_AU"],
    "New_Zealand": ["Auckland", "Wellington", "Christchurch", "Hamilton_NZ", "Tauranga", "Napier", "Dunedin"],
    "Papua_New_Guinea": ["Port_Moresby", "Lae", "Mount_Hagen", "Madang", "Wewak"],
    "Fiji": ["Suva", "Lautoka", "Nadi", "Labasa"],

    # Cities (leaves)
    "Lagos": [], "Abuja": [], "Kano": [], "Ibadan": [], "Port_Harcourt": [],
    "Cairo": [], "Alexandria": [], "Giza": [], "Luxor": [], "Aswan": [],
    "Cape_Town": [], "Johannesburg": [], "Durban": [], "Pretoria": [], "Port_Elizabeth": [],
    "Nairobi": [], "Mombasa": [], "Kisumu": [], "Nakuru": [], "Eldoret": [],
    "Casablanca": [], "Rabat": [], "Marrakech": [], "Fez": [], "Tangier": [],
    "Addis_Ababa": [], "Dire_Dawa": [], "Mekelle": [], "Gondar": [], "Hawassa": [],
    "Accra": [], "Kumasi": [], "Tamale": [], "Sekondi": [], "Cape_Coast": [],
    "Algiers": [], "Oran": [], "Constantine": [], "Annaba": [], "Blida": [],
    "Kampala": [], "Gulu": [], "Lira": [], "Mbarara": [], "Jinja": [],
    "Dar_es_Salaam": [], "Dodoma": [], "Mwanza": [], "Arusha": [], "Mbeya": [],
    "Dakar": [], "Touba": [], "Thies": [], "Kaolack": [], "Ziguinchor": [],
    "Abidjan": [], "Bouake": [], "Daloa": [], "Yamoussoukro": [], "Korhogo": [],
    "Douala": [], "Yaounde": [], "Garoua": [], "Kousseris": [], "Bamenda": [],
    "Maputo": [], "Matola": [], "Nampula": [], "Beira": [], "Chimoio": [],
    "Antananarivo": [], "Toamasina": [], "Antsirabe": [], "Fianarantsoa": [], "Mahajanga": [],
    "Luanda": [], "Huambo": [], "Lobito": [], "Benguela": [], "Namibe": [],
    "Lusaka": [], "Kitwe": [], "Ndola": [], "Kabwe": [], "Chingola": [],
    "Harare": [], "Bulawayo": [], "Chitungwiza": [], "Mutare": [], "Gweru": [],
    "Kigali": [], "Butare": [], "Gitarama": [], "Musanze": [], "Byumba": [],
    "Khartoum": [], "Omdurman": [], "Port_Sudan": [], "Kassala": [], "El_Obeid": [],
    "Beijing": [], "Shanghai": [], "Guangzhou": [], "Shenzhen": [], "Chengdu": [], "Hangzhou": [], "Wuhan": [], "Xian": [], "Nanjing": [], "Tianjin": [],
    "Mumbai": [], "Delhi": [], "Bangalore": [], "Hyderabad": [], "Chennai": [], "Kolkata": [], "Pune": [], "Ahmedabad": [], "Jaipur": [], "Surat": [],
    "Tokyo": [], "Osaka": [], "Kyoto": [], "Yokohama": [], "Kobe": [], "Nagoya": [], "Sapporo": [], "Fukuoka": [], "Hiroshima": [], "Sendai": [],
    "Seoul": [], "Busan": [], "Incheon": [], "Daegu": [], "Daejeon": [], "Gwangju": [], "Ulsan": [], "Suwon": [],
    "Bangkok": [], "Chiang_Mai": [], "Phuket": [], "Pattaya": [], "Hat_Yai": [], "Nakhon_Ratchasima": [],
    "Ho_Chi_Minh_City": [], "Hanoi": [], "Da_Nang": [], "Hai_Phong": [], "Can_Tho": [], "Hue": [],
    "Jakarta": [], "Surabaya": [], "Bandung": [], "Medan": [], "Semarang": [], "Makassar": [], "Palembang": [],
    "Kuala_Lumpur": [], "George_Town": [], "Ipoh": [], "Shah_Alam": [], "Petaling_Jaya": [], "Johor_Bahru": [],
    "Manila": [], "Quezon_City": [], "Davao": [], "Cebu_City": [], "Zamboanga": [], "Antipolo": [],
    "Singapore_City": [],
    "Karachi": [], "Lahore": [], "Faisalabad": [], "Rawalpindi": [], "Multan": [], "Islamabad": [], "Peshawar": [], "Quetta": [],
    "Dhaka": [], "Chittagong": [], "Sylhet": [], "Rajshahi": [], "Khulna": [], "Comilla": [],
    "Naypyidaw": [], "Yangon": [], "Mandalay": [], "Mawlamyine": [], "Bago": [],
    "Phnom_Penh": [], "Siem_Reap": [], "Battambang": [], "Sihanoukville": [], "Kampong_Cham": [],
    "Colombo": [], "Kandy": [], "Galle": [], "Jaffna": [], "Negombo": [],
    "Kathmandu": [], "Pokhara": [], "Lalitpur": [], "Bharatpur": [], "Birgunj": [],
    "Tashkent": [], "Samarkand": [], "Namangan": [], "Andijan": [], "Nukus": [],
    "Almaty": [], "Astana": [], "Shymkent": [], "Karaganda": [], "Aktobe": [],
    "Baku": [], "Ganja": [], "Sumqayit": [], "Mingachevir": [], "Nakhchivan": [],
    "Tbilisi": [], "Kutaisi": [], "Batumi": [], "Rustavi": [], "Gori": [],
    "Istanbul": [], "Ankara": [], "Izmir": [], "Bursa": [], "Adana": [], "Gaziantep": [], "Konya": [], "Antalya": [],
    "Tehran": [], "Mashhad": [], "Isfahan": [], "Tabriz": [], "Shiraz": [], "Ahvaz": [], "Qom": [], "Karaj": [],
    "Riyadh": [], "Jeddah": [], "Mecca": [], "Medina": [], "Dammam": [], "Taif": [], "Tabuk": [],
    "Baghdad": [], "Basra": [], "Mosul": [], "Erbil": [], "Kirkuk": [], "Najaf": [], "Karbala": [],
    "Jerusalem": [], "Tel_Aviv": [], "Haifa": [], "Rishon_LeZion": [], "Petah_Tikva": [], "Ashdod": [], "Beer_Sheva": [],
    "Amman": [], "Zarqa": [], "Irbid": [], "Russeifa": [], "Wadi_as_Seer": [],
    "Beirut": [], "Tripoli_Lebanon": [], "Sidon": [], "Tyre": [], "Jounieh": [],
    "Dubai": [], "Abu_Dhabi": [], "Sharjah": [], "Al_Ain": [], "Ajman": [],
    "Doha": [], "Al_Rayyan": [], "Al_Wakrah": [], "Al_Khor": [],
    "Kuwait_City": [], "Hawalli": [], "Salmiya": [], "Ahmadi": [],
    "Muscat": [], "Salalah": [], "Sohar": [], "Nizwa": [], "Sur": [],
    "Kabul": [], "Kandahar": [], "Herat": [], "Mazar_e_Sharif": [], "Jalalabad": [],
    "Damascus": [], "Aleppo": [], "Homs": [], "Latakia": [], "Hama": [],
    "Sanaa": [], "Aden": [], "Taiz": [], "Al_Hudaydah": [], "Ibb": [],
    "Manama": [], "Riffa": [], "Muharraq": [], "Hamad_Town": [],
    "Berlin": [], "Munich": [], "Hamburg": [], "Cologne": [], "Frankfurt": [], "Stuttgart": [], "Dusseldorf": [], "Dortmund": [], "Essen": [], "Leipzig": [],
    "Paris": [], "Marseille": [], "Lyon": [], "Toulouse": [], "Nice": [], "Nantes": [], "Strasbourg": [], "Montpellier": [], "Bordeaux": [], "Lille": [],
    "London": [], "Birmingham": [], "Manchester": [], "Glasgow": [], "Liverpool": [], "Leeds": [], "Sheffield": [], "Edinburgh": [], "Bristol": [], "Cardiff": [],
    "Rome": [], "Milan": [], "Naples": [], "Turin": [], "Palermo": [], "Genoa": [], "Bologna": [], "Florence": [], "Bari": [], "Catania": [],
    "Madrid": [], "Barcelona": [], "Valencia": [], "Seville": [], "Zaragoza": [], "Malaga": [], "Murcia": [], "Palma": [], "Las_Palmas": [], "Bilbao": [],
    "Warsaw": [], "Krakow": [], "Lodz": [], "Wroclaw": [], "Poznan": [], "Gdansk": [], "Szczecin": [], "Bydgoszcz": [], "Lublin": [], "Katowice": [],
    "Amsterdam": [], "Rotterdam": [], "The_Hague": [], "Utrecht": [], "Eindhoven": [], "Tilburg": [], "Groningen": [], "Almere": [],
    "Brussels": [], "Antwerp": [], "Ghent": [], "Charleroi": [], "Liege": [], "Bruges": [], "Namur": [], "Leuven": [],
    "Athens": [], "Thessaloniki": [], "Patras": [], "Heraklion": [], "Larissa": [], "Volos": [], "Rhodes": [], "Ioannina": [],
    "Lisbon": [], "Porto": [], "Vila_Nova_de_Gaia": [], "Amadora": [], "Braga": [], "Funchal": [], "Coimbra": [], "Setubal": [],
    "Stockholm": [], "Gothenburg": [], "Malmo": [], "Uppsala": [], "Vasteras": [], "Orebro": [], "Linkoping": [],
    "Oslo": [], "Bergen": [], "Trondheim": [], "Stavanger": [], "Drammen": [], "Fredrikstad": [],
    "Copenhagen": [], "Aarhus": [], "Odense": [], "Aalborg": [], "Esbjerg": [], "Randers": [],
    "Helsinki": [], "Espoo": [], "Tampere": [], "Vantaa": [], "Oulu": [], "Turku": [],
    "Zurich": [], "Geneva": [], "Basel": [], "Bern": [], "Lausanne": [], "Winterthur": [],
    "Vienna": [], "Graz": [], "Linz": [], "Salzburg": [], "Innsbruck": [], "Klagenfurt": [],
    "Prague": [], "Brno": [], "Ostrava": [], "Plzen": [], "Liberec": [], "Olomouc": [],
    "Budapest": [], "Debrecen": [], "Miskolc": [], "Szeged": [], "Pecs": [], "Gyor": [],
    "Bucharest": [], "Cluj_Napoca": [], "Timisoara": [], "Iasi": [], "Constanta": [], "Craiova": [],
    "Kyiv": [], "Kharkiv": [], "Odessa": [], "Dnipro": [], "Donetsk": [], "Lviv": [], "Zaporizhzhia": [],
    "Belgrade": [], "Novi_Sad": [], "Nis": [], "Kragujevac": [], "Subotica": [],
    "Zagreb": [], "Split": [], "Rijeka": [], "Osijek": [], "Zadar": [],
    "Dublin": [], "Cork": [], "Limerick": [], "Galway": [], "Waterford": [],
    "New_York": [], "Los_Angeles": [], "Chicago": [], "Houston": [], "Phoenix": [], "Philadelphia": [], "San_Antonio": [], "San_Diego": [], "Dallas": [], "San_Jose": [],
    "Austin": [], "Jacksonville": [], "Fort_Worth": [], "Columbus": [], "Charlotte": [], "Indianapolis": [], "San_Francisco": [], "Seattle": [], "Denver": [], "Nashville": [],
    "Toronto": [], "Montreal": [], "Vancouver": [], "Calgary": [], "Edmonton": [], "Ottawa": [], "Winnipeg": [], "Quebec_City": [], "Hamilton": [], "Kitchener": [],
    "Mexico_City": [], "Guadalajara": [], "Monterrey": [], "Puebla": [], "Tijuana": [], "Leon": [], "Juarez": [], "Torreon": [], "Queretaro": [], "Merida": [],
    "Guatemala_City": [], "Mixco": [], "Villa_Nueva": [], "Petapa": [], "San_Juan_Sacatepequez": [],
    "Havana": [], "Santiago_de_Cuba": [], "Camaguey": [], "Holguin": [], "Santa_Clara": [],
    "Kingston": [], "Spanish_Town": [], "Portmore": [], "Montego_Bay": [], "May_Pen": [],
    "Cartago": [], "Puntarenas": [], "Limon": [], "Alajuela": [],
    "Panama_City": [], "San_Miguelito": [], "Tocumen": [], "David": [], "Arraijan": [],
    "Tegucigalpa": [], "San_Pedro_Sula": [], "Choloma": [], "La_Ceiba": [], "El_Progreso": [],
    "Managua": [], "Masaya": [], "Matagalpa": [], "Chinandega": [],
    "Santo_Domingo": [], "Santiago_DR": [], "La_Romana": [], "San_Pedro_de_Macoris": [], "Puerto_Plata": [],
    "Port_au_Prince": [], "Cap_Haitien": [], "Gonaives": [], "Saint_Marc": [], "Petionville": [],
    "San_Salvador": [], "Soyapango": [], "Santa_Ana": [], "San_Miguel": [], "Mejicanos": [],
    "Port_of_Spain": [], "San_Fernando": [], "Chaguanas": [], "Arima": [],
    "Sao_Paulo": [], "Rio_de_Janeiro": [], "Brasilia": [], "Salvador": [], "Fortaleza": [], "Belo_Horizonte": [], "Manaus": [], "Curitiba": [], "Recife": [], "Porto_Alegre": [],
    "Buenos_Aires": [], "Cordoba": [], "Rosario": [], "Mendoza": [], "Tucuman": [], "La_Plata": [], "Mar_del_Plata": [], "Salta": [], "Santa_Fe": [], "San_Juan": [],
    "Santiago": [], "Valparaiso": [], "Concepcion": [], "La_Serena": [], "Antofagasta": [], "Temuco": [], "Rancagua": [], "Talca": [], "Arica": [], "Chilian": [],
    "Lima": [], "Arequipa": [], "Trujillo": [], "Chiclayo": [], "Huancayo": [], "Piura": [], "Iquitos": [], "Cusco": [], "Chimbote": [], "Tacna": [],
    "Bogota": [], "Medellin": [], "Cali": [], "Barranquilla": [], "Cartagena": [], "Cucuta": [], "Bucaramanga": [], "Pereira": [], "Santa_Marta": [], "Ibague": [],
    "Caracas": [], "Maracaibo": [], "Barquisimeto": [], "Maracay": [], "Ciudad_Guayana": [], "San_Cristobal": [], "Maturin": [], "Ciudad_Bolivar": [], "Cumana": [],
    "Quito": [], "Guayaquil": [], "Cuenca": [], "Santo_Domingo_EC": [], "Machala": [], "Manta": [], "Portoviejo": [], "Ambato": [], "Riobamba": [], "Esmeraldas": [],
    "Montevideo": [], "Salto": [], "Paysandu": [], "Las_Piedras": [], "Rivera": [],
    "Asuncion": [], "Ciudad_del_Este": [], "San_Lorenzo": [], "Luque": [], "Capiata": [],
    "La_Paz": [], "Santa_Cruz": [], "Cochabamba": [], "Oruro": [], "Sucre": [],
    "Georgetown": [], "Linden": [], "New_Amsterdam": [], "Anna_Regina": [],
    "Paramaribo": [], "Lelydorp": [], "Nieuw_Nickerie": [], "Moengo": [],
    "Sydney": [], "Melbourne": [], "Brisbane": [], "Perth": [], "Adelaide": [], "Canberra": [], "Darwin": [], "Hobart": [], "Gold_Coast": [], "Newcastle_AU": [],
    "Auckland": [], "Wellington": [], "Christchurch": [], "Hamilton_NZ": [], "Tauranga": [], "Napier": [], "Dunedin": [],
    "Port_Moresby": [], "Lae": [], "Mount_Hagen": [], "Madang": [], "Wewak": [],
    "Suva": [], "Lautoka": [], "Nadi": [], "Labasa": [],
    # More European cities
    "Slovakia": ["Bratislava", "Kosice", "Presov", "Zilina", "Nitra"],
    "Bulgaria": ["Sofia", "Plovdiv", "Varna", "Burgas", "Ruse"],
    "Belarus": ["Minsk", "Gomel", "Mogilev", "Vitebsk", "Grodno"],
    "Lithuania": ["Vilnius", "Kaunas", "Klaipeda", "Siauliai", "Panevezys"],
    "Latvia": ["Riga", "Daugavpils", "Liepaja", "Jelgava", "Jurmala"],
    "Estonia": ["Tallinn", "Tartu", "Narva", "Parnu", "Kohtla_Jarve"],
    "Slovenia": ["Ljubljana", "Maribor", "Celje", "Kranj", "Velenje"],
    "Bratislava": [], "Kosice": [], "Presov": [], "Zilina": [], "Nitra": [],
    "Sofia": [], "Plovdiv": [], "Varna": [], "Burgas": [], "Ruse": [],
    "Minsk": [], "Gomel": [], "Mogilev": [], "Vitebsk": [], "Grodno": [],
    "Vilnius": [], "Kaunas": [], "Klaipeda": [], "Siauliai": [], "Panevezys": [],
    "Riga": [], "Daugavpils": [], "Liepaja": [], "Jelgava": [], "Jurmala": [],
    "Tallinn": [], "Tartu": [], "Narva": [], "Parnu": [], "Kohtla_Jarve": [],
    "Ljubljana": [], "Maribor": [], "Celje": [], "Kranj": [], "Velenje": [],
    # More Central America
    "Belize": ["Belmopan", "Belize_City", "San_Ignacio", "Orange_Walk", "Dangriga"],
    "Belmopan": [], "Belize_City": [], "San_Ignacio": [], "Orange_Walk": [], "Dangriga": [],
    # More Oceania
    "Solomon_Islands": ["Honiara", "Gizo", "Auki", "Kirakira"],
    "Vanuatu": ["Port_Vila", "Luganville", "Isangel", "Sola"],
    "Samoa": ["Apia", "Asau", "Mulifanua", "Afega"],
    "Kiribati": ["South_Tarawa", "Betio", "Bikenibeu", "Eita"],
    "Tonga": ["Nukualofa", "Neiafu", "Haveluloto", "Vaini"],
    "Honiara": [], "Gizo": [], "Auki": [], "Kirakira": [],
    "Port_Vila": [], "Luganville": [], "Isangel": [], "Sola": [],
    "Apia": [], "Asau": [], "Mulifanua": [], "Afega": [],
    "South_Tarawa": [], "Betio": [], "Bikenibeu": [], "Eita": [],
    "Nukualofa": [], "Neiafu": [], "Haveluloto": [], "Vaini": [],
}

DICTIONARIES = {
    "elements": ELEMENTS,
    "solar": SOLAR_SYSTEM,
    "greek_myth": GREEK_MYTHOLOGY,
    "geo": GEOGRAPHY,
}


@dataclass
class BeadSpec:
    """A plan for a single bead to be created."""
    name: str
    kind: str
    input_indices: List[int] = field(default_factory=list)
    extra_file_count: int = 0


def die(message):
    """Print an error message to stderr and exit."""
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def assign_names_and_kinds(args: argparse.Namespace) -> List[BeadSpec]:
    """Create initial bead specs with names and kinds assigned."""
    dictionary = DICTIONARIES[args.dictionary]
    selected_names = random.sample(list(dictionary.keys()), args.num_names)
    
    plan: List[BeadSpec] = []
    # Ensure each selected name is used at least once
    for name in selected_names:
        plan.append(BeadSpec(name=name, kind=""))
    # Fill the rest with random names from the selected set
    for _ in range(args.num_beads - args.num_names):
        plan.append(BeadSpec(name=random.choice(selected_names), kind=""))

    # Assign kinds
    kinds = [f"kind-{i+1}" for i in range(args.num_kinds)]
    # Ensure each kind is used at least once
    for i, kind_name in enumerate(kinds):
        plan[i].kind = kind_name
    # Fill the rest with random kinds
    for i in range(args.num_kinds, args.num_beads):
        plan[i].kind = random.choice(kinds)

    random.shuffle(plan)
    return plan


def assign_inputs(plan: List[BeadSpec], args: argparse.Namespace) -> None:
    """Assign input dependencies to beads using exponential distribution."""
    dictionary = DICTIONARIES[args.dictionary]
    
    # Build name-to-indices mapping for finding related beads
    name_to_indices: Dict[str, List[int]] = {}
    for i, spec in enumerate(plan):
        if spec.name not in name_to_indices:
            name_to_indices[spec.name] = []
        name_to_indices[spec.name].append(i)

    possible_input_providers = []
    remaining_inputs = args.num_inputs
    
    for i, spec in enumerate(plan):
        if remaining_inputs > 0 and possible_input_providers:
            max_inputs_for_this_bead = min(remaining_inputs, len(possible_input_providers))
            if max_inputs_for_this_bead > 0:
                num_inputs_to_add = min(max_inputs_for_this_bead, 
                                      max(0, int(random.expovariate(2.0))))
                
                if num_inputs_to_add > 0:
                    candidates = _find_input_candidates(
                        spec, dictionary, name_to_indices, possible_input_providers)
                    
                    for _ in range(num_inputs_to_add):
                        if candidates and remaining_inputs > 0:
                            input_idx = candidates.pop(0)
                            spec.input_indices.append(input_idx)
                            remaining_inputs -= 1
        
        possible_input_providers.append(i)


def _find_input_candidates(spec: BeadSpec, dictionary: Dict[str, List[str]], 
                          name_to_indices: Dict[str, List[int]], 
                          possible_input_providers: List[int]) -> List[int]:
    """Find candidate input providers, prioritizing related beads."""
    related_names = dictionary.get(spec.name, [])
    related_predecessors = []
    
    if related_names:
        for rel_name in related_names:
            for idx in name_to_indices.get(rel_name, []):
                if idx in possible_input_providers:
                    related_predecessors.append(idx)
    
    # Add related inputs first, then random ones
    random.shuffle(related_predecessors)
    random.shuffle(possible_input_providers)
    
    return related_predecessors + [idx for idx in possible_input_providers 
                                  if idx not in related_predecessors]


def assign_extra_files(plan: List[BeadSpec], num_extra_files: int) -> None:
    """Distribute extra files across beads using exponential distribution."""
    if num_extra_files == 0:
        return
    
    # Generate exponential weights for each bead
    weights = [random.expovariate(1.0) for _ in plan]
    total_weight = sum(weights)
    
    # Distribute files proportionally to weights
    remaining_files = num_extra_files
    for i, weight in enumerate(weights[:-1]):  # Handle all but last bead
        files_for_this_bead = int(remaining_files * weight / total_weight)
        plan[i].extra_file_count = files_for_this_bead
        remaining_files -= files_for_this_bead
        total_weight -= weight
    
    # Give remaining files to the last bead
    plan[-1].extra_file_count = remaining_files


def create_plan(args: argparse.Namespace) -> List[BeadSpec]:
    """Generate the ordered plan for bead creation."""
    plan = assign_names_and_kinds(args)
    assign_inputs(plan, args)
    assign_extra_files(plan, args.num_extra_files)
    return plan


def execute_plan(plan: List[BeadSpec], archive_output_dir: Path, workspaces_dir: Path):
    """Execute the plan to create beads, add files, and clean up."""
    generated_archive_paths: List[Path] = []
    
    print(f"Generating {len(plan)} beads in {archive_output_dir}...")

    try:
        # Step 1: Create Bead Archives
        for i, spec in enumerate(plan):
            workspace_path = workspaces_dir / f"{spec.name}_{i}"
            
            # Create a fresh workspace for each bead
            ws = Workspace(str(workspace_path))
            ws.create(spec.kind)

            # Add dummy content
            workspace_output_dir = Path(str(ws.directory)) / layouts.Workspace.OUTPUT
            (workspace_output_dir / "placeholder.txt").write_text(f"content for {spec.name} #{i}")

            # Add inputs
            for input_index in spec.input_indices:
                input_path = generated_archive_paths[input_index]
                input_name = plan[input_index].name

                # Count how many times this name has been used as input already
                existing_count = sum(1 for existing_name in ws.meta.get('inputs', {}).keys()
                                    if existing_name == input_name or existing_name.startswith(f"{input_name}_"))

                # First occurrence gets the plain name, subsequent ones get indexed
                if existing_count == 0:
                    local_input_name = input_name
                else:
                    local_input_name = f"{input_name}_{existing_count}"

                archive = ZipArchive(str(input_path))
                ws.load(local_input_name, archive)

            # Save the bead
            freeze_time = timestamp.timestamp()
            archive_filename = f"{spec.name}_{freeze_time}.zip"
            archive_path = archive_output_dir / archive_filename
            ws.pack(archive_path, freeze_time, comment="")
            generated_archive_paths.append(archive_path)
            print(f"  ({i+1}/{len(plan)}) Created {archive_path.name}")
            
            # Clean up this workspace immediately
            fs.rmtree(workspace_path)

        # Step 2: Add Extra Files
        print("\nAdding extra files to archives...")
        for i, spec in enumerate(plan):
            if spec.extra_file_count > 0:
                archive_path = generated_archive_paths[i]
                add_extra_files_to_archive(archive_path, spec.extra_file_count)
    finally:
        # Step 3: Cleanup
        print("\nCleaning up...")
        if workspaces_dir.exists():
            fs.rmtree(workspaces_dir)
            print(f"  Removed temporary directory {workspaces_dir}")


def add_extra_files_to_archive(archive_path: Path, count: int):
    """Add a number of empty extra files to a bead archive."""
    with zipfile.ZipFile(archive_path, 'a') as zf:
        for j in range(count):
            zf.writestr(f"extra-files/{j}", "")
    print(f"  Added {count} files to {archive_path.name}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Generate bead archives with specific properties.")
    parser.add_argument(
        "-d", "--dictionary",
        default='solar',
        choices=DICTIONARIES.keys(),
        help="Dictionary to use for names."
    )
    parser.add_argument("-n", "--num-names", type=int, default=1, help="Number of different names for beads. (default=%(default)s)")
    parser.add_argument("-k", "--num-kinds", type=int, default=1, help="Number of different kinds for beads. (default=%(default)s)")
    parser.add_argument("-i", "--num-inputs", type=int, default=0, help="Total number of inputs across all beads. (default=%(default)s)")
    parser.add_argument("-x", "--num-extra-files", type=int, default=0, help="Total number of extra files to add. (default=%(default)s)")
    parser.add_argument("num_beads", type=int, help="Total number of beads to create.")
    parser.add_argument("output_directory", type=Path, help="Directory to add beads to.")
    
    args = parser.parse_args()

    # Validation
    dictionary = DICTIONARIES[args.dictionary]
    if args.num_names > len(dictionary):
        die(f"Number of names ({args.num_names}) cannot be greater than dictionary size ({len(dictionary)}).")
    if args.num_beads < args.num_names:
        die(f"Number of beads ({args.num_beads}) must be >= number of names ({args.num_names}).")
    if args.num_beads < args.num_kinds:
        die(f"Number of beads ({args.num_beads}) must be >= number of kinds ({args.num_kinds}).")

    # Directory setup
    archive_output_dir = args.output_directory
    archive_output_dir.mkdir(parents=True, exist_ok=True)
    workspaces_dir = archive_output_dir / ".workspaces"
    if workspaces_dir.exists():
        fs.rmtree(workspaces_dir)
    workspaces_dir.mkdir()

    # Create and execute the plan
    plan = create_plan(args)
    execute_plan(plan, archive_output_dir, workspaces_dir)
    
    print("\nDone.")


if __name__ == "__main__":
    main()
