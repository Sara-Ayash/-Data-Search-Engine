import os, sys, re, io, time
from pathlib import Path
import collections
import linecache
from itertools import islice
from io import DEFAULT_BUFFER_SIZE
import traceback 

from Globals import BLOCK_SIZE
from Globals import NUMBER_OF_LINES
 
try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

class Compression(object):
    def __init__(self,dir):
        # Check if 'dir' is valid directory
        Path(dir).mkdir(parents=True, exist_ok=True)
        self.index_dir = dir 
        self.compression_tokens()     
      
    def compression_tokens(self):
        self.index_in_file = 0 
        self.map_file = ""
        token = ""
        all_fiels = os.listdir(self.index_dir) 
        for file in all_fiels:
            if re.match("(tokens\d+)", file) and file.endswith('.txt'): 
                token = file     
      
        path = os.path.join(self.index_dir, token)  
        path_to = os.path.join(self.index_dir, "tokens.bin")
        path_map_file = os.path.join(self.index_dir,"map_tokens.txt")     

        with open(path, "r") as path_from, open(path_to, "ab") as compression_file, open(path_map_file, "a") as map_file:
            str_map_file = ""
            
            counter = 0
            try:
                for line in path_from:
                    counter += 1
                    new_file = line.split()
                    item = str(new_file[0])
                    posting_list = [int(num) for num in new_file[1:]]
                    varint_posting_list =  b''
                    varint_posting_list += encode(posting_list[0]) 
                    varint_posting_list += encode(posting_list[1]) 
                    posting_dict = self.sort_posting_list(posting_list[2:])
                   
                    gap = 0  
                    start = time.time()
                    size_varint_posting_list = 0
                    for key, value in posting_dict.items(): 
                        gap = int(key) - gap  
                        varint_posting_list += encode(gap) 
                        varint_posting_list += encode(value) 
                        gap = int(key)
                        if len(varint_posting_list) > 500:
                            size_varint_posting_list += len(varint_posting_list)
                            compression_file.write(varint_posting_list)
                            varint_posting_list = b''
 
                    size_varint_posting_list += len(varint_posting_list) 
                    new_line = '{} {} {}\n'.format(str(item), self.index_in_file, size_varint_posting_list)
                    self.index_in_file += size_varint_posting_list
                    str_map_file += new_line
                    
                    if sys.getsizeof(str_map_file) > 1000: 
                        map_file.write(str(str_map_file))
                        str_map_file = ""
                
                    compression_file.write(varint_posting_list)
                    varint_posting_list = b''
                    
                map_file.write(str(str_map_file))
                        
            except BaseException as err:
                traceback.print_exc()
               
 
        os.remove(path)   
   
    def posting_list_gaps(self, posting_list):
        varint_posting_list =  b''
        varint_posting_list += encode(posting_list[0]) 
        varint_posting_list += encode(posting_list[1]) 
        posting_dict = self.sort_posting_list(posting_list[2:])
        temp = b''
        gap = 0 
        counter = 0 
        start = time.time()
        for key, value in posting_dict.items(): 
            counter += 1
            varint_posting_list += encode(int(key) - gap) 
            varint_posting_list += encode(value) 
            gap = int(key)
            end = time.time() - start
            if end > 2:
                temp += varint_posting_list
                start = time.time()
                varint_posting_list = b''
               
        return temp + varint_posting_list
 
    def sort_posting_list(self, posting_list):
        posting_dict = {}
        for index in range(0, len(posting_list), 2):
            posting_dict[posting_list[index]] = posting_list[index + 1]      
        return collections.OrderedDict(sorted(posting_dict.items())) 

    def compression_products(self):
        self.index_in_file = 0 
        self.map_file = ""
        product = "products1.txt"       
        all_fiels = os.listdir(self.index_dir) 
        for file in all_fiels:
            if re.match("(products\d+)", file): 
                product = file
                  
        path = os.path.join(self.index_dir, product)  
        path_to = os.path.join(self.index_dir, "products.bin")
        path_map_file = os.path.join(self.index_dir,"map_products.txt")
        with open(path, "r") as path_from, open(path_to, "ab") as compression_file, open(path_map_file, "w") as map_file:
            block = list(islice(path_from, NUMBER_OF_LINES))
            while block:
                compressed_block = self.compression_file_block(block) # compressed block
                compression_file.write(compressed_block) # write the compressed block to disk
                map_file.write(self.map_file)
                self.map_file = ''
                block = list(islice(path_from, NUMBER_OF_LINES)) # reed new block
        os.remove(path)
            
    def compression_file_block(self, block):
        binary_varint_block = b''
        for line in block:
            new_file = line.split()
            item = str(new_file[0])
            posting_list = [int(num) for num in new_file[1:]]
           
            varint_posting_list =  b''
            for index in posting_list: # get posting_list as varint binary string
                varint_posting_list += encode(index) 
            size_varint_posting_list = len(varint_posting_list) 
            
            new_line = '{} {} {}\n'.format(str(item), self.index_in_file, size_varint_posting_list)
            self.map_file += new_line
            self.index_in_file += size_varint_posting_list
            binary_varint_block += varint_posting_list
         
        return binary_varint_block

    def dict_to_string(self):
        string_dict = ''
        for key, item in self.map_file.items():
            string_dict += "{} {} {}\n".format(key, item[0],item[1])
        return string_dict

   

def encode(number):
    """ Pack `number` into varint bytes"""
    buf = b''
    while True:
        towrite = number & 0x7f 
        number >>= 7
        if number:
            buf += bytes((towrite | 0x80, ))
        else:
            buf += _byte(towrite)
            break
    return buf

def _byte(b):
        return bytes((b, ))

def decode_stream(stream):
    """Read a varint from `stream`"""
    
    shift = 0
    result = 0
    list_results =[]
    while True:
        i = _read_one(stream)
        if not i:
            break
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            list_results.append(result)
            result = 0
            shift = 0

            

    return list_results

def decode_bytes(buf):
    """Read a varint from from `buf` bytes"""
    return decode_stream(BytesIO(buf))

def _read_one(stream):
    """Read a byte from the file (as an integer)
    raises EOFError if the stream ends while reading bytes.
    """
    c = stream.read(1)
    if c == b'':
        return
    return ord(c)
        



