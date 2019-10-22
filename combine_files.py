import os
print("Starting file combiner")
directory_file = os.listdir(os.getcwd())
file_data = open(os.path.join(os.getcwd(),'combined_index'),'w+')
file_list = []

for item in directory_file:
	if (item.split(".")[-1]!='crc'): file_list.append(item)

for files in file_list:
	print('Combining the file : '+files)
	current_file = open(os.path.join(os.getcwd(), files),'r')
	current_file_data = current_file.read()
	file_data.write(current_file_data)
	current_file.close()

file_data.close()
print("Closing file combiner")