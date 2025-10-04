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
    "Sun": ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Ceres", "Pluto", "Haumea", "Makemake", "Eris", "Vesta", "Pallas", "Hygiea", "Halley", "Encke", "Hale_Bopp"],
    "Mercury": [], "Venus": [],
    "Earth": ["Moon"], "Moon": [],
    "Mars": ["Phobos", "Deimos"], "Phobos": [], "Deimos": [],
    "Ceres": [],
    "Jupiter": ["Io", "Europa", "Ganymede", "Callisto", "Amalthea", "Himalia", "Lysithea", "Elara", "Pasiphae", "Sinope", "Carme", "Ananke", "Leda", "Thebe", "Adrastea", "Metis"], 
    "Io": [], "Europa": [], "Ganymede": [], "Callisto": [], "Amalthea": [], "Himalia": [], "Lysithea": [], "Elara": [], "Pasiphae": [], "Sinope": [], "Carme": [], "Ananke": [], "Leda": [], "Thebe": [], "Adrastea": [], "Metis": [],
    "Saturn": ["Mimas", "Enceladus", "Tethys", "Dione", "Rhea", "Titan", "Iapetus", "Hyperion", "Phoebe", "Janus", "Epimetheus", "Helene", "Telesto", "Calypso", "Atlas", "Prometheus", "Pandora", "Pan", "Daphnis"], 
    "Mimas": [], "Enceladus": [], "Tethys": [], "Dione": [], "Rhea": [], "Titan": [], "Iapetus": [], "Hyperion": [], "Phoebe": [], "Janus": [], "Epimetheus": [], "Helene": [], "Telesto": [], "Calypso": [], "Atlas": [], "Prometheus": [], "Pandora": [], "Pan": [], "Daphnis": [],
    "Uranus": ["Ariel", "Umbriel", "Titania", "Oberon", "Miranda", "Cordelia", "Ophelia", "Bianca", "Cressida", "Desdemona", "Juliet", "Portia", "Rosalind", "Belinda", "Puck"], 
    "Ariel": [], "Umbriel": [], "Titania": [], "Oberon": [], "Miranda": [], "Cordelia": [], "Ophelia": [], "Bianca": [], "Cressida": [], "Desdemona": [], "Juliet": [], "Portia": [], "Rosalind": [], "Belinda": [], "Puck": [],
    "Neptune": ["Triton", "Nereid", "Proteus", "Larissa", "Galatea", "Despina", "Thalassa", "Naiad"], 
    "Triton": [], "Nereid": [], "Proteus": [], "Larissa": [], "Galatea": [], "Despina": [], "Thalassa": [], "Naiad": [],
    "Pluto": ["Charon", "Nix", "Hydra", "Styx", "Kerberos"], 
    "Charon": [], "Nix": [], "Hydra": [], "Styx": [], "Kerberos": [],
    "Haumea": ["Namaka", "Hiʻiaka"], "Namaka": [], "Hiʻiaka": [],
    "Makemake": [],
    "Eris": ["Dysnomia"], "Dysnomia": [],
    # Asteroids
    "Vesta": [], "Pallas": [], "Hygiea": [],
    # Comets
    "Halley": [], "Encke": [], "Hale_Bopp": [],
}

GREEK_MYTHOLOGY = {
    "Chaos": ["Gaia", "Tartarus", "Eros", "Erebus", "Nyx"],
    "Gaia": ["Uranus", "Pontus", "Ourea"], "Uranus": ["Titans", "Cyclopes", "Hecatoncheires"],
    "Titans": ["Oceanus", "Tethys", "Hyperion", "Theia", "Coeus", "Phoebe", "Cronus", "Rhea", "Mnemosyne", "Themis", "Crius", "Iapetus"],
    "Cronus": ["Hestia", "Hades", "Demeter", "Poseidon", "Hera", "Zeus"], "Rhea": ["Hestia", "Hades", "Demeter", "Poseidon", "Hera", "Zeus"],
    "Zeus": ["Athena", "Apollo", "Artemis", "Ares", "Hephaestus", "Hebe", "Eileithyia", "Persephone", "Dionysus", "Hermes"],
    "Hera": ["Ares", "Hephaestus", "Hebe", "Eileithyia"],
    "Poseidon": ["Triton", "Polyphemus", "Theseus"],
    "Hades": ["Zagreus", "Macaria"],
    "Demeter": ["Persephone"],
    "Apollo": ["Asclepius", "Orpheus"],
    "Athena": ["Erichthonius"],
    "Ares": ["Phobos", "Deimos", "Eros", "Anteros"],
    "Aphrodite": ["Eros", "Anteros", "Hermaphroditus", "Aeneas"],
    "Hermes": ["Pan", "Abderus"],
    "Hephaestus": [], "Hestia": [], "Artemis": [],
    "Iapetus": ["Atlas", "Prometheus", "Epimetheus", "Menoetius"],
    "Prometheus": ["Deucalion"],
    "Atlas": ["Hesperides", "Hyades", "Pleiades"],
}

GEOGRAPHY = {
    # Continents
    "Africa": ["Nigeria", "Egypt", "South_Africa", "Kenya", "Morocco", "Ethiopia", "Ghana", "Algeria", "Uganda", "Tanzania"],
    "Asia": ["China", "India", "Japan", "South_Korea", "Thailand", "Vietnam", "Indonesia", "Malaysia", "Philippines", "Singapore"],
    "Europe": ["Germany", "France", "United_Kingdom", "Italy", "Spain", "Poland", "Netherlands", "Belgium", "Greece", "Portugal"],
    "North_America": ["United_States", "Canada", "Mexico", "Guatemala", "Cuba", "Jamaica", "Costa_Rica", "Panama", "Honduras", "Nicaragua"],
    "South_America": ["Brazil", "Argentina", "Chile", "Peru", "Colombia", "Venezuela", "Ecuador", "Uruguay", "Paraguay", "Bolivia"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra", "Darwin", "Hobart"],
    "Antarctica": [],
    
    # Countries and their major cities
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
    
    "United_States": ["New_York", "Los_Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San_Antonio", "San_Diego", "Dallas", "San_Jose"],
    "Canada": ["Toronto", "Montreal", "Vancouver", "Calgary", "Edmonton", "Ottawa", "Winnipeg", "Quebec_City", "Hamilton", "Kitchener"],
    "Mexico": ["Mexico_City", "Guadalajara", "Monterrey", "Puebla", "Tijuana", "Leon", "Juarez", "Torreon", "Queretaro", "Merida"],
    "Guatemala": ["Guatemala_City", "Mixco", "Villa_Nueva", "Petapa", "San_Juan_Sacatepequez"],
    "Cuba": ["Havana", "Santiago_de_Cuba", "Camaguey", "Holguin", "Santa_Clara"],
    "Jamaica": ["Kingston", "Spanish_Town", "Portmore", "Montego_Bay", "May_Pen"],
    "Costa_Rica": ["San_Jose", "Cartago", "Puntarenas", "Limon", "Alajuela"],
    "Panama": ["Panama_City", "San_Miguelito", "Tocumen", "David", "Arraijan"],
    "Honduras": ["Tegucigalpa", "San_Pedro_Sula", "Choloma", "La_Ceiba", "El_Progreso"],
    "Nicaragua": ["Managua", "Leon", "Masaya", "Matagalpa", "Chinandega"],
    
    "Brazil": ["Sao_Paulo", "Rio_de_Janeiro", "Brasilia", "Salvador", "Fortaleza", "Belo_Horizonte", "Manaus", "Curitiba", "Recife", "Porto_Alegre"],
    "Argentina": ["Buenos_Aires", "Cordoba", "Rosario", "Mendoza", "Tucuman", "La_Plata", "Mar_del_Plata", "Salta", "Santa_Fe", "San_Juan"],
    "Chile": ["Santiago", "Valparaiso", "Concepcion", "La_Serena", "Antofagasta", "Temuco", "Rancagua", "Talca", "Arica", "Chilian"],
    "Peru": ["Lima", "Arequipa", "Trujillo", "Chiclayo", "Huancayo", "Piura", "Iquitos", "Cusco", "Chimbote", "Tacna"],
    "Colombia": ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena", "Cucuta", "Bucaramanga", "Pereira", "Santa_Marta", "Ibague"],
    "Venezuela": ["Caracas", "Maracaibo", "Valencia", "Barquisimeto", "Maracay", "Ciudad_Guayana", "San_Cristobal", "Maturin", "Ciudad_Bolivar", "Cumana"],
    "Ecuador": ["Quito", "Guayaquil", "Cuenca", "Santo_Domingo", "Machala", "Manta", "Portoviejo", "Ambato", "Riobamba", "Esmeraldas"],
    "Uruguay": ["Montevideo", "Salto", "Paysandu", "Las_Piedras", "Rivera"],
    "Paraguay": ["Asuncion", "Ciudad_del_Este", "San_Lorenzo", "Luque", "Capiata"],
    "Bolivia": ["La_Paz", "Santa_Cruz", "Cochabamba", "Oruro", "Sucre"],
    
    # Cities without further subdivisions
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
    "New_York": [], "Los_Angeles": [], "Chicago": [], "Houston": [], "Phoenix": [], "Philadelphia": [], "San_Antonio": [], "San_Diego": [], "Dallas": [], "San_Jose": [],
    "Toronto": [], "Montreal": [], "Vancouver": [], "Calgary": [], "Edmonton": [], "Ottawa": [], "Winnipeg": [], "Quebec_City": [], "Hamilton": [], "Kitchener": [],
    "Mexico_City": [], "Guadalajara": [], "Monterrey": [], "Puebla": [], "Tijuana": [], "Leon": [], "Juarez": [], "Torreon": [], "Queretaro": [], "Merida": [],
    "Guatemala_City": [], "Mixco": [], "Villa_Nueva": [], "Petapa": [], "San_Juan_Sacatepequez": [],
    "Havana": [], "Santiago_de_Cuba": [], "Camaguey": [], "Holguin": [], "Santa_Clara": [],
    "Kingston": [], "Spanish_Town": [], "Portmore": [], "Montego_Bay": [], "May_Pen": [],
    "Cartago": [], "Puntarenas": [], "Limon": [], "Alajuela": [],
    "Panama_City": [], "San_Miguelito": [], "Tocumen": [], "David": [], "Arraijan": [],
    "Tegucigalpa": [], "San_Pedro_Sula": [], "Choloma": [], "La_Ceiba": [], "El_Progreso": [],
    "Managua": [], "Masaya": [], "Matagalpa": [], "Chinandega": [],
    "Sao_Paulo": [], "Rio_de_Janeiro": [], "Brasilia": [], "Salvador": [], "Fortaleza": [], "Belo_Horizonte": [], "Manaus": [], "Curitiba": [], "Recife": [], "Porto_Alegre": [],
    "Buenos_Aires": [], "Cordoba": [], "Rosario": [], "Mendoza": [], "Tucuman": [], "La_Plata": [], "Mar_del_Plata": [], "Salta": [], "Santa_Fe": [], "San_Juan": [],
    "Santiago": [], "Valparaiso": [], "Concepcion": [], "La_Serena": [], "Antofagasta": [], "Temuco": [], "Rancagua": [], "Talca": [], "Arica": [], "Chilian": [],
    "Lima": [], "Arequipa": [], "Trujillo": [], "Chiclayo": [], "Huancayo": [], "Piura": [], "Iquitos": [], "Cusco": [], "Chimbote": [], "Tacna": [],
    "Bogota": [], "Medellin": [], "Cali": [], "Barranquilla": [], "Cartagena": [], "Cucuta": [], "Bucaramanga": [], "Pereira": [], "Santa_Marta": [], "Ibague": [],
    "Caracas": [], "Maracaibo": [], "Barquisimeto": [], "Maracay": [], "Ciudad_Guayana": [], "San_Cristobal": [], "Maturin": [], "Ciudad_Bolivar": [], "Cumana": [],
    "Quito": [], "Guayaquil": [], "Cuenca": [], "Santo_Domingo": [], "Machala": [], "Manta": [], "Portoviejo": [], "Ambato": [], "Riobamba": [], "Esmeraldas": [],
    "Montevideo": [], "Salto": [], "Paysandu": [], "Las_Piedras": [], "Rivera": [],
    "Asuncion": [], "Ciudad_del_Este": [], "San_Lorenzo": [], "Luque": [], "Capiata": [],
    "La_Paz": [], "Santa_Cruz": [], "Cochabamba": [], "Oruro": [], "Sucre": [],
    "Sydney": [], "Melbourne": [], "Brisbane": [], "Perth": [], "Adelaide": [], "Canberra": [], "Darwin": [], "Hobart": [],
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
                existing_count = sum(1 for existing_nick in ws.meta.get('inputs', {}).keys() 
                                    if existing_nick == input_name or existing_nick.startswith(f"{input_name}_"))
                
                # First occurrence gets the plain name, subsequent ones get indexed
                if existing_count == 0:
                    input_nick = input_name
                else:
                    input_nick = f"{input_name}_{existing_count}"
                
                archive = ZipArchive(str(input_path))
                ws.load(input_nick, archive)

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
