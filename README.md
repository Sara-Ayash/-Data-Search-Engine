# -Data-Search-Engine
 Data search engine in a web environment
 
IndexWriter.py​- Given product review data, inputFile is the
path to the file containing the review data dir is the directory in which all index files will be
created

this class creates an on disk index in 3 steps:
1. generate text files of tokens dictionary and products dictionary. each time the
memory usage is bigger then BLOCK_SIZE two files of token and products are
created after all the files are written, the class call to Merge class.
2. Merge class merge all the tokens file into single file and so on to products.
3. after the merge the class call to Compress class which compress the big files in the
following way: the product file is separated to product id and tokens file
if the directory does not exist, it should be created

Merge.py​- merge the tokens and product files created on indexwriter class

Compress.py​- comress tokens and product files.

IndexReader.py​​- Creates an IndexReader which will read from the given directory

