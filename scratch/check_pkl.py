import pickle
import os

DB_PATH = r"D:\ViettelCloudCamera\demo_ai\db_faces"
pkl_file_path = None
for file in os.listdir(DB_PATH):
    if file.endswith(".pkl"):
        pkl_file_path = os.path.join(DB_PATH, file)
        break

if pkl_file_path:
    print(f"Loading {pkl_file_path}...")
    with open(pkl_file_path, 'rb') as f:
        data = pickle.load(f)
    print("Type of data:", type(data))
    if len(data) > 0:
        print("Type of first item:", type(data[0]))
        print("First item contents/keys:")
        if isinstance(data[0], dict):
            print(data[0].keys())
        elif isinstance(data[0], list):
            print("First item is a list of length:", len(data[0]))
            if len(data[0]) > 0:
                print("Type of first subitem:", type(data[0][0]))
                if isinstance(data[0][0], dict):
                    print("Subitem keys:", data[0][0].keys())
                else:
                    print("Subitem:", data[0][0])
        else:
            print(data[0])
else:
    print("No .pkl file found!")
