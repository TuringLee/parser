import os
import shutil
import zipfile
import rarfile
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('input_file_path')
parser.add_argument('output_file_path')
args = parser.parse_args()
input_path = args.input_file_path
output_path = args.output_file_path

def decompress_zip_file(file):
    zip_file = zipfile.ZipFile(input_path+'/'+file)
    os.mkdir(output_path+'/'+file_num)
    for subfile in zip_file.namelist():  
        zip_file.extract(subfile,output_path + '/' +file_num)  
    zip_file.close()

def decompress_rar_file(file):
    rar_file = rarfile.RarFile(input_path+'/'+file)
    os.mkdir(output_path+'/'+file_num)
    for subfile in rar_file.namelist():
        rar_file.extract(subfile,output_path+'/'+file_num)
    rar_file.close()

file_list = os.listdir(input_path)

for file in file_list:

    print file,
    
    file_num = file.split('.')[0]
    file_format = file.split('.')[1]

    if file_format == 'zip':
        try:
            zip_file = zipfile.ZipFile(input_path+'/'+file)
        except Exception , e:
            os.rename(input_path+'/'+file,input_path+'/'+file_num+'.rar')
            file = file_num + '.rar'
            decompress_rar_file(file)
            print 'down after change the format'
        else:
            os.mkdir(output_path+'/'+file_num)
            for subfile in zip_file.namelist():
                zip_file.extract(subfile,output_path + '/' +file_num)  
            zip_file.close()
            print 'Down without change the format .'

    elif file_format == 'rar':
        try:
            rar_file = rarfile.RarFile(input_path+'/'+file)
        except Exception , e:
            os.rename(input_path+'/'+file,input_path+'/'+file_num+'.zip')
            file = file_num + '.zip'
            decompress_zip_file(file)
            print 'down after change the format'
        else:
            os.mkdir(output_path+'/'+file_num)  
            for subfile in rar_file.namelist():
                rar_file.extract(subfile,output_path+'/'+file_num)
            rar_file.close()
            print 'Down without change the format .'
    elif file_format == 'srt':
        os.mkdir(output_path+'/'+file_num)
        shutil.copy(input_path+'/'+file,output_path+'/'+file_num)

    else:
        
        continue

    for sub in os.listdir(output_path+'/'+file_num):
        if os.path.isdir(output_path+'/'+file_num+'/'+sub):
            shutil.copytree(output_path+'/'+file_num+'/'+sub, output_path+'/'+file_num+'_')
            shutil.rmtree(output_path+'/'+file_num)

print "ALL DOWN!"  
