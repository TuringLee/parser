import os
import zipfile
import rarfile
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('input_file_path')
parser.add_argument('output_file_path')
args = parser.parse_args()
input_path = args.input_file_path
output_path = args.output_file_path

file_list = os.listdir(input_path)

for file in file_list:

    print file
    
    file_num = file.split('.')[0]
    file_format = file.split('.')[1]

    if file_format == 'zip':
        zip_file = zipfile.ZipFile(input_path+'/'+file)
        os.mkdir(output_path+'/'+file_num)
        for subfile in zip_file.namelist():  
            zip_file.extract(subfile,output_path + '/' +file_num)  
        zip_file.close()

    elif file_format == 'rar':
        rar_file = rarfile.RarFile(input_path+'/'+file)
        os.mkdir(output_path+'/'+file_num)
        for subfile in rar_file.namelist():
            rar_file.extract(subfile,output_path+'/'+file_num)
        rar_file.close()
    
    else:
        continue

print "Down!"  



