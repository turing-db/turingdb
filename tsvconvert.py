#!/usr/bin/python3

import sys
import csv
import json


def decomment(csvfile):
    for row in csvfile:
        raw = row.split('#')[0].strip()
        if raw:
            yield row


if len(sys.argv) < 3:
    print("ERROR: please give a data file and the name of an output file !")
    print("USAGE: tsvconvert.py data.tsv output.json")
    sys.exit(1)

fd = open(sys.argv[1])
reader = csv.reader(decomment(fd), delimiter='\t')

write_file = open(sys.argv[2], "w+")

entities = []

print("* Reading data")

for row in reader:
    ID = row[0]
    name = row[1]
    reference = row[2]
    formula = row[3]
    charge = row[4]
    mass = row[5]
    inchi = row[6]
    inchikey = row[7]
    smiles = row[8]

    entities.append({"id": ID, "name": name, "ref": reference, 
        "formula": formula, "charge": charge, "mass": mass, "inchi": inchi,
        "inchikey": inchikey, "smiles": smiles})

print("* Writing json file")
json.dump(entities, write_file)
