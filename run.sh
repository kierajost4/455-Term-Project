#!/bin/bash
gradle build

bash ./scripts/runGISCrime.sh

bash ./scripts/institutionsPerCapita.sh child_care_centers.csv careCenters
bash ./scripts/institutionsPerCapita.sh hospitals.csv hospitals
bash ./scripts/institutionsPerCapita.sh local_law_enforcement_locations.csv localLaw
bash ./scripts/institutionsPerCapita.sh places_of_worship.csv worship
bash ./scripts/institutionsPerCapita.sh private_schools.csv privateSchools
bash ./scripts/institutionsPerCapita.sh public_schools.csv publicSchools
