from Extract_Crime import extract_crime_main
from Extract_Economics import extract_acs

if __name__ == '__main__':
    paths = []
    path = extract_crime_main()
    paths.append(path)
    
    path = extract_acs()
    for file in path:
        paths.append(path[file])