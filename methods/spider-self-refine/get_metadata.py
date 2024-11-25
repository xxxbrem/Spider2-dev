import os
import json
import shutil

def save_to_jsonl(folder_names, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        for name in folder_names:
            line = {'instance_id': name, "answer_type": "file", "answer_or_path": "output.csv"}
            file.write(json.dumps(line, ensure_ascii=False) + '\n')

def get_csv_from_dic(folder_names, output_dic):
    for name in folder_names:
        name = name+"/result.csv"
        if os.path.exists(directory + '/' + name):
            path_csv = os.path.join(directory, name)
            shutil.copy(path_csv, os.path.join(output_dic, "result.csv"))

directory = "output/o1-preview-test1"
# output_dic = "o1_results_for_lite_test"
folder_names = [name for name in os.listdir(directory) if os.path.isdir(os.path.join(directory, name))]
save_to_jsonl(folder_names, directory+'/results_metadata.jsonl')
# get_csv_from_dic(folder_names, output_dic)

