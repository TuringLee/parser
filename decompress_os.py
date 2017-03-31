import os
import shutil
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

    fr_decompressed = open(input_path+'/'+'decompressed_index.txt','a+') 
    log_file = open(output_path+'/'+'log_file.txt','a+')

    decompress_index = fr_decompressed.readlines()
    
    file_num = file.split('.')[0]
    file_format = file.split('.')[-1]
    if file_num+'\n' in decompress_index:
        print file_num + 'has decompressed!'
        continue

    if file_format == 'zip':
        if not (os.path.exists(output_path+'/'+file_num) or os.path.exists(output_path+'/'+file_num+'_')):
            os.mkdir(output_path+'/'+file_num)
            os.system('unzip -o '+input_path+'/'+file+' -d '+output_path+'/'+file_num)
            print 'down'
        else:
            print file_num + ' something unexcepted occured , Ignore this file. _1'
            log_file.write(file_num + ' something unexcepted occured , Ignore this file. _1\n')
            continue

    elif file_format == 'rar':
        if not (os.path.exists(output_path+'/'+file_num) or os.path.exists(output_path+'/'+file_num+'_')):
            os.mkdir(output_path+'/'+file_num)
            os.system('unrar e -o+ '+input_path+'/'+file+' '+output_path+'/'+file_num)
            print 'down'
        else:
            print file_num + ' something unexcepted occured , Ignore this file. _2'
            log_file.write(file_num + ' something unexcepted occured , Ignore this file. _2\n')
            continue

    elif file_format == 'srt':
        if not (os.path.exists(output_path+'/'+file_num) or os.path.exists(output_path+'/'+file_num+'_')):
            os.mkdir(output_path+'/'+file_num)
            shutil.copy(input_path+'/'+file,output_path+'/'+file_num)
    
    else:
        continue

    if not os.listdir(output_path+'/'+file_num):
        print file_num + ' something unexcepted occured , Ignore this file. _3'
        log_file.write(file_num + ' something unexcepted occured , Ignore this file. _3\n')
        shutil.rmtree(output_path+'/'+file_num)
        continue
    
    for sub in os.listdir(output_path+'/'+file_num):
        if os.path.isdir(output_path+'/'+file_num+'/'+sub):
            if not os.path.exists(output_path+'/'+file_num+'_'):
                shutil.copytree(output_path+'/'+file_num+'/'+sub, output_path+'/'+file_num+'_')
                shutil.rmtree(output_path+'/'+file_num)
    
    fr_decompressed.write(file_num)
    fr_decompressed.write('\n')
    fr_decompressed.close()
    log_file.close()

print "ALL DOWN!"  
