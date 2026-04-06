#!/usr/bin/bash

if [[ -z "$1" ]]; then
  echo "No collection group file supplied. Exiting."
  exit
fi

while IFS= read -r line; do
  ./export_objects_by_collection.py $line
done < $1 
