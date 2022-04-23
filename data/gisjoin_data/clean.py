import os
import json




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
                            f.write(entry['GISJOIN'] +","+ state_name + "," + str(entry['properties']['NAMELSAD10']) + '\n')

if __name__ == "__main__":
    main()