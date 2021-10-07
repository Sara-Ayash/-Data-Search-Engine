import os, sys, re, io, time
import collections
import shutil
import trace
from Compression import Compression
from Merge import Merge
from pathlib import Path
from itertools import islice
from Globals import BLOCK_SIZE
from Globals import NUMBER_OF_LINES

PRODUCT = "product/productId: "
HELPFULNESS = "review/helpfulness: "
SCORE = "review/score: "
TEXT = "review/text: "


class IndexWriter(object): 
    def __init__(self, inputFile, dir): 
        """Given product review data, creates an on disk index 
        inputFile is the path to the file containing the review 
        data, dir is the directory in which all index files will 
        be created if the directory does not exist, it should 
        be created""" 

        #Check if 'inputFile' is exist path 
        if os.path.exists(inputFile):
            self.input_file = inputFile 
        else:
            print("Input file doesn't exists")
            return 
            
        start = time.time()
        # Check if 'dir' is valid directory 
        Path(dir).mkdir(parents=True, exist_ok=True)  
        
        print("\nstart writer files...")
        start = time.time()
        # class veriables
        self.index_dir = dir 
        self.num_of_reviews = 0
        self.num_of_tokens = 0
        self.num_of_all_tokens = 0
        self.ordinal_number = 0
        self.last_prod = ""
        self.map_review = ""
        self.num_write_disk = 0
        self.product_id_list = []
        self.token_dict = {}
        self.read_input() # Move to function read_input to  
        self.num_write_disk = 101
        self.flash_to_memory()
        self.write_general_data()
        Merge(dir)
        Compression(dir) 

    def read_input(self): 
        review = []
        required_fields =  [PRODUCT, HELPFULNESS, SCORE, TEXT]
        current_fields =  [PRODUCT, HELPFULNESS, SCORE, TEXT]
        numOfInCorrenct = 0
        # Make single review with only the required fields and send him to function 'split_review()' 
        with open(self.input_file) as input:
            for line in input:  
                    
                if PRODUCT in line or len(review) == 4: # If start new review 
                    if  PRODUCT in line:
                        numOfInCorrenct += 1
                    if len(review) == 4:
                        self.split_review(review) # The function get review and make the index
                          
                    review = []
                    current_fields =  [PRODUCT, HELPFULNESS, SCORE, TEXT] 
                # Append only the required fields
                if current_fields[0] in line:
                    review.append(line)
                    current_fields.pop(0)
                 
    def split_review(self, review):
        """ The function receives a new review and updates the files 
        required to store the reviews collection in the new information """
        if sys.getsizeof(self.token_dict) > BLOCK_SIZE:
            self.flash_to_memory() 
        self.build_reviews_file(review)
        for line in review:
            if PRODUCT in line: 
                self.build_product_file(line)
            elif TEXT in line:
                self.build_token_file(line)
       
    def flash_to_memory(self):
        self.ordinal_number += 1
        self.write_text_tokens()
        self.num_write_disk += 1
        self.token_dict = {}
       
    def build_reviews_file(self, full_review):
        """ The function adds the reviews with the data only with no 
        headings into the review_list """
        if not full_review:
            return
        
        self.num_of_reviews += 1
        text = full_review[3].split(TEXT)[-1].splitlines()[0]
        splited_text = re.sub('[^0-9a-zA-Z]+', ' ', text)
        text_length = len(splited_text.split())
        self.num_of_all_tokens += text_length
        
    def bit_length(self):
        s = bin(self)       # binary representation:  bin(-37) --> '-0b100101'
        s = s.lstrip('-0b') # remove leading zeros and minus sign
        return len(s)       # len('100101') --> 6

    def build_product_file(self, product_id):
        """
        check if the ProductID exist in product_id_list 
        if exist - add only the current review - num_of_reviews 
        if not exist - make new array and add the reviewId
        """  
        product_id = product_id.split(PRODUCT)[1].splitlines()[0]
        if not product_id in self.product_id_list:
            self.product_id_list.append(product_id)
            
    def build_token_file(self, review_text):
        text = review_text.split(TEXT)[-1].splitlines()[0]
        splited_text = re.sub('[^0-9a-zA-Z]+', ' ', text)
        splited_text = splited_text.lower()
        for token in splited_text.split():
            if not token in self.token_dict:
                self.token_dict[token] = {}
            key_exist = [key for key in self.token_dict[token] if key == self.num_of_reviews]
            if key_exist:
                self.token_dict[token][self.num_of_reviews] += 1 
            else:
                self.token_dict[token][self.num_of_reviews] = 1
       
    def removeIndex(self, dir): 
        """Delete all index files by removing the given directory""" 
        shutil.rmtree(dir, ignore_errors=True)

    def write_text_tokens(self):
        # Save tokens file 
        order_index = collections.OrderedDict(sorted(self.token_dict.items()))   
        number_of_words = 0
        for token, reviews in order_index.items():
            for index, value in reviews.items():
                number_of_words += value
                
        file_string = ""
        for token, reviews in order_index.items(): 
            frequency = 0
            for index, value in reviews.items():
                frequency += value
            reviews_str = ' '.join(str(review) + " " + str(frequency) for review, frequency in reviews.items())
            token_list = "{} {} {} {}\n".format(token, len(reviews), frequency, reviews_str)
            file_string += token_list 
        
        current_file = os.path.join(self.index_dir, "tokens" + str(self.ordinal_number) + ".txt")
        with open(current_file, "w") as tokens_file:
            tokens_file.write(file_string)  
    
    def write_general_data(self):
        general_file = os.path.join(self.index_dir, "general.txt") 
        help_fullness_text = "" 
        with open(general_file, "w") as general_file:
            general_file.write("{}\n".format(self.num_of_reviews))
            general_file.write("{}\n".format(self.num_of_all_tokens))
            general_file.write("{}\n".format(len(self.product_id_list)))
        
            
        