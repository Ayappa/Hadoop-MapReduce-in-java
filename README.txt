
1. The index.java ,fullIndex.java and FullIndexTwoWord.java take two folders as input arguments which are input folder and output folder

Arguments
Input Output

2. The QueryIndex.java ,QueryFullIndex.java and QuerryFullIndexTwo.java take three arguments the names of three files as stated below.

  Arguments
QueryFileName IndexFileName OutputFileName 

3. As MapReduce files are split into many ,we need to combine it to single file, so run combine_files.py to merge all reduce 
   file as one and give combined_index.txt for reading querryfile for comparing querry and Index file. Copy the combine_files.py to output folder.

