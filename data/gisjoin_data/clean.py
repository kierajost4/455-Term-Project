import os
import json

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New_Hampshire": "NH",
    "New_Jersey": "NJ",
    "New_Mexico": "NM",
    "New_York": "NY",
    "North_Carolina": "NC",
    "North_Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode_Island": "RI",
    "South_Carolina": "SC",
    "South_Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West_Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District_of_Columbia": "DC",
    "American_Samoa": "AS",
    "Guam": "GU",
    "Northern_Mariana_Islands": "MP",
    "Puerto_Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}

def main():
    print("Starting...")
    directory = '.'
 
    # iterate over files in
    # that directory
    paths = []
    count = 0
    with open('cleaned_meta_data.csv', "w+") as f:
        for subdir, dirs, files in os.walk(directory):
            #print(files)
            for file in files:
                file_path = os.path.join(subdir, file)
                if (file_path[-19:] == 'linkedGeometry.json'):
                    state_name = file_path.split('.')[2]
                    with open(file_path, "r") as f2:
                        jdata = json.loads(f2.read())
                        for entry in jdata:
                            f.write(entry['GISJOIN'] +","+ state_name + "," + us_state_to_abbrev[state_name] +","+ str(entry['properties']['NAMELSAD10']) + '\n')
    print("Done!")
if __name__ == "__main__":
    main()