import os
folder_path = "./output/o1-preview-test1"
# folder_path = "./output/test"
# folder_path = "/root/repo/Spider2-dev/spider2/evaluation_suite/gold"
# folder_path = "examples"
for filename in os.listdir(folder_path):
    if filename.startswith("sf_"):
        old_file = os.path.join(folder_path, filename)
        new_file = os.path.join(folder_path, ''.join(filename.split('_')[1:]))
        os.rename(old_file, new_file)

# for filename in os.listdir(folder_path):
#     if not filename.startswith("sf_"):
#         old_file = os.path.join(folder_path, filename)
#         new_file = os.path.join(folder_path, 'sf_' + filename)
#         os.rename(old_file, new_file)