import os, sys, re, io, time
from pathlib import Path
import collections, traceback
from itertools import islice, zip_longest
from io import DEFAULT_BUFFER_SIZE
# BLOCK_SIZE = 11000
# NUMBER_OF_LINES = 800

from Globals import BLOCK_SIZE
from Globals import NUMBER_OF_LINES

class Merge(object):
    def __init__(self, dir):
        # Check if 'dir' is valid directory
        Path(dir).mkdir(parents=True, exist_ok=True)
        self.num_of_token = 0
        self.num_of_product = 0
        self.index_dir = dir
        self.merge_tokens()
        self.merge_products()
        self.write_general()
        
        
    def write_general(self):
        path_general = os.path.join(self.index_dir, "general.txt")
        with open(path_general, "a") as general:
            general.write(str(self.num_of_token) + "\n")
            # general.write(str(self.num_of_product) + "\n")

    
    def merge_tokens_line(self, line1, line2):
        line1 = line1.strip("\n").split()
        line2 = line2.strip("\n").split()
        item = line1[0]
        posting_list_len = int(line1[1]) + int(line2[1])
        total_frequency = int(line1[2]) + int(line2[2])
        new_token_line = [item, posting_list_len, total_frequency]
        # concat the posting lists
        if line1[3] > line2[3]:
            new_token_line += line1[3:]  
            
            new_token_line += line2[3:]  
        else: 
            new_token_line += line2[3:]
            new_token_line += line1[3:]
        return (' '.join(str(item) for item in new_token_line) + "\n")


    def merge_products_line(self, line1, line2):
        line1 = line1.strip("\n").split()
        line2 = line2.strip("\n").split()
        item = line1[0]
        posting_list_len = int(line1[1]) + int(line2[1])
        new_product_line = [item, posting_list_len]
        # concat the posting lists
        posting_list =   line1[2:] + line1[2:]
        new_product_line += sorted(posting_list)
        
        return (' '.join(str(item) for item in new_product_line) + "\n")


    def compare_lines(self, line1, line2):
        line1_elements = line1.split()
        line2_elements = line2.split() 
        if line1_elements[0] > line2_elements[0]:
            return 2
        if line1_elements[0] < line2_elements[0]:
            return 1
        return 3

    
    def merge_tokens(self):
       
        all_fiels = os.listdir(self.index_dir)
        tokens = [] # tokens files
        for file in all_fiels:
            if re.match("(tokens\d+)", file):
                tokens.append(file)
        while len(tokens) > 1:
            for index in range(0, len(tokens)-1, 2):
                self.num_of_token = 0
                start = time.time()
                self.new_merge(tokens[index], tokens[index+1], files_type="tokens")
                time_compress = time.time() - start
            all_fiels = os.listdir(self.index_dir)
            tokens = [] # tokens files
            for file in all_fiels:
                if re.match("(tokens\d+)", file):
                    tokens.append(file)

    def new_merge(self, file1_name, file2_name, files_type="tokens"): 
        equals = 0
        merged_file = os.path.join(self.index_dir, "temp.txt")
        sorted_lines = []
        append_write = "w"
        path_file1 = os.path.join(self.index_dir, file1_name)
        path_file2 = os.path.join(self.index_dir, file2_name)
       
        with open(path_file1, "r") as file1, open(path_file2, "r") as file2:
            line1 = next(file1)
            line2 = next(file2)
            while line1 and line2:
                add_line = self.compare_lines(line1, line2)
                if add_line == 1:
                    sorted_lines.append(line1)
                    try:
                        line1 = next(file1)
                    except:
                        line1 = None
                elif add_line == 2:
                    sorted_lines.append(line2)
                    try:
                        line2 = next(file2)
                    except:
                        line2 = None
                else:

                    if files_type == "tokens":
                        new_line = self.merge_tokens_line(line1, line2)
                    else:
                        new_line = self.merge_products_line(line1, line2)
                    sorted_lines.append(new_line)
                    try:
                        line1 = next(file1)
                    except:
                        line1 = None
                    try:
                        line2 = next(file2)
                    except:
                        line2 = None
                if sys.getsizeof(sorted_lines) > BLOCK_SIZE:
                    if os.path.exists(merged_file):
                        append_write = 'a' # append if already exists
                    else:
                        append_write = 'w' # make a new file if not
                    with open(merged_file, append_write) as merged:
                        if files_type == "tokens":
                            self.num_of_token += len(sorted_lines)
                        else:
                            self.num_of_product += len(sorted_lines)
                        merged.writelines(sorted_lines)
                    sorted_lines = []
            while line1:
                try:
                    sorted_lines.append(line1)
                    line1 = next(file1)        
                except:
                    break
                
            # dump rest of file1                    
            while line2:
                try:
                    sorted_lines.append(line2)
                    line2 = next(file2)   
                except:
                    break

        if os.path.exists(merged_file):
            append_write = 'a' # append if already exists
        else:
            append_write = 'w' # make a new file if not
        with open(merged_file, append_write) as merged:
            if files_type == "tokens":
                self.num_of_token += len(sorted_lines)
            else:
                self.num_of_product += len(sorted_lines)
            merged.writelines(sorted_lines)
        
        os.remove(path_file1) 
        os.remove(path_file2)
        try:
            if os.path.exists(path_file1) : os.remove(path_file1) 
            if os.path.exists(path_file2) : os.remove(path_file2)
            if not os.path.exists(path_file1):
                os.rename(merged_file, path_file1)
            else:
                time.sleep(4)
                if os.path.exists(path_file1): os.remove(path_file1)
                os.rename(merged_file, path_file1)
        except BaseException as err:
            time.sleep(4)
            try:
                if os.path.exists(path_file1): os.remove(path_file1)
                if os.path.exists(path_file2): os.remove(path_file2)
                os.rename(merged_file, path_file2)
            except BaseException as err:
                pass 
               
                
    def merge_products(self):
        all_fiels = os.listdir(self.index_dir)
        products = [] # products files
        for file in all_fiels:
            if re.match("(products\d+)", file):
                products.append(file)
        while len(products) > 1:
            for index in range(0, len(products)-1, 2):
                self.num_of_product = 0
                self.new_merge(products[index], products[index+1], files_type="products")
            all_fiels = os.listdir(self.index_dir)
            products = [] # products files
            for file in all_fiels:
                if re.match("(products\d+)", file):
                    products.append(file)


