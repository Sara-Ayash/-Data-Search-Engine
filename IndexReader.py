import os, sys, time
from pathlib import Path  
import linecache
import traceback 
import collections
try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO
TOKEN_FILE = 11645
BYTE_SIZE = 8

class IndexReader(object):
    def __init__(self, dir): 
        """Creates an IndexReader which will read from the given directory""" 
        # Check if 'dir' is valid directory 
        Path(dir).mkdir(parents=True, exist_ok=True)  
        self.index_dir = dir 
        self.num_of_reviews = None
        self.num_of_all_token = None
        self.num_of_token = None
        self.num_of_product = None
        self.read_general_file()     
   
    def getTokenFrequency(self, token):    
        """Return the number of reviews containing a given token (i.e., word) Returns 0 if there are no reviews containing this token""" 
        token = token.lower()
        line = self.find_line(token, "token")
        # print(line)
        if line:
            decode_line = self.decode_line(line, "token")
            return decode_line[0]
        return 0
     
    def getTokenCollectionFrequency(self, token):    
        """Return the number of times that a given token (i.e., word) appears in the reviews indexed Returns 0 if there are no reviews containing this token""" 
        token = token.lower()
        line = self.find_line(token, "token")
        if line:
            decode_line = self.decode_line(line, "token")
            return decode_line[1]
        return 0
     
    def getReviewsWithToken(self, token):    
        """Returns a series of integers (Tupple) of the form id-1, freq-1, id-2, freq-2,
         ... such that id-n is the n-th review containing the given token and freq-n is the
          number of times that the token appears in review id-n Note that the integers should be sorted by id 
        Returns an empty Tupple if there are no reviews containing this token""" 
        token = token.lower()
        line = self.find_line(token, "token")
        if line:
            decode_line = self.decode_line(line, "token")
            posting_list = decode_line[2:] 
            posting_list = self.convert_from_gaps(posting_list)
            return tuple(posting_list)
        return ()
     
    def getNumberOfReviews(self):    
        """Return the number of product reviews available in the system""" 
        return self.num_of_reviews
     
    def getTokenSizeOfReviews(self):    
        """Return the number of tokens in the system (Tokens should be counted as many times as they appear)""" 
        number_of_tokens = linecache.getline(os.path.join(self.index_dir, "general.txt"), 2) 
        return int(number_of_tokens)

    def getProductId(self, reviewId):    
        """Returns the product identifier for the given review Returns null 
        if there is no review with the given identifier"""
        if not self.valid_review(reviewId):
            return
        index = int(reviewId)  
        line = linecache.getline(os.path.join(self.index_dir, "map_review.txt"), index)
        if not line:
            return
        while True:
            prod_id = line.split()[0]
            if prod_id != "0":
                return prod_id
            else: 
                index -= 1
                line = linecache.getline(os.path.join(self.index_dir, "map_review.txt"), index)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
    def convert_from_gaps(self, posting_list):
        # print(posting_list)
        gap = 0
        sum_gap = 0
        for index in range(0, len(posting_list), 2):
            gap = posting_list[index]
            posting_list[index] += sum_gap  
            sum_gap += gap
        return posting_list

    def read_general_file(self):
        with open(os.path.join(self.index_dir, "general.txt"), "r") as general:
            general_data = general.readlines()
        self.num_of_reviews = int(general_data[0])
        self.num_of_all_token = int(general_data[1])
        self.num_of_product = int(general_data[2])
        self.num_of_token = int(general_data[3])

    def valid_review(self, review_id):
        try:
            review_id = int(review_id)
            if review_id < 1:
                return
            if review_id > self.num_of_reviews:
                return
        except: 
            return 
        return True
        
    def number_to_binary(self, number, padding=10):
            return bin(int(number))[2:].zfill(padding) 

    def binary_to_number(self, binary, padding=10):
        try:
            to_ret = str(int(binary, 2)).zfill(padding)
        except:
            return 0
        return to_ret

    def string_to_binary(self, string):
        return ''.join(bin(c)[2:].zfill(8)  for c in string.encode('ascii'))
    
    def binary_to_string(self,binary):
        string = ""
        for index in range(0, len(binary), 8):
            current = binary[index:index+8]
            string += int(current, 2).to_bytes(1, 'big').decode('ascii')
        return string    

    def find_review_line(self, review_id):
        with open(os.path.join(self.index_dir, "compression_reviews.bin"), "rb") as compression_reviews:
            compression_reviews.seek(5*(review_id-1))
            reader = compression_reviews.read(5) 
            return reader         
    
    def find_line(self, item, item_type):
        if item_type == "token":
            file_name = os.path.join(self.index_dir, "map_tokens.txt")
            # get num of tokens from general file
            numOfItems = linecache.getline(os.path.join(self.index_dir, "general.txt"), 4)
        else:
            file_name = os.path.join(self.index_dir, "map_products.txt")
            # get num of tokens from general file
            numOfItems = linecache.getline(os.path.join(self.index_dir, "general.txt"), 5)
        if not numOfItems:
            return
        numOfItems = int(numOfItems)
        left = 1
        item = str(item)
        rigtht = numOfItems
        while left <= rigtht:
            mid = left + int((rigtht-left)//2)
            line = linecache.getline(file_name, mid)
            item_in_line = line.split()[0]
            if item_in_line > item:
                rigtht = mid - 1
            elif item_in_line < item:
                left = mid + 1
            else:
                return line
                  
    def decode_line(self, line, item_type):
        token_data = line.split()
        byte_index = int(token_data[1])
        byte_read = int(token_data[2])
        if item_type == "token":
            file_name = "tokens.bin" 
        else:
            file_name = "products.bin" 
        with open(os.path.join(self.index_dir, file_name), "rb") as compression_item:
            compression_item.seek(byte_index)
            reader = compression_item.read(byte_read)
            decoding_line = decode_bytes(reader)
            return decoding_line


# ------------- not need --------------------------------   
    def getReviewScore(self, reviewId):    
        """Returns the score for a given review Returns -1 if there 
        is no review with the given identifier""" 
        if not self.valid_review(reviewId):
            return   
        review = self.find_review_line(int(reviewId)) 
        score = int.from_bytes(review[2:3], byteorder='big')
        if score:
            return score

    def getReviewHelpfulnessNumerator(self, reviewId):    
        """Returns the numerator for the helpfulness of a given review 
        Returns -1 if there is no review with the given identifier""" 
        if not self.valid_review(reviewId):
            return   
        review = self.find_review_line(int(reviewId))
        numerator = int.from_bytes(review[0:1], byteorder='big')
        denominator = int.from_bytes(review[1:2], byteorder='big')
        if denominator == 0 and numerator != 0:
            line = linecache.getline(os.path.join(self.index_dir, "map_review.txt"), reviewId)
            numerator = line.split()[1]
       
        return numerator

    def getReviewHelpfulnessDenominator(self, reviewId):    
        """Returns the denominator for the helpfulness of a given 
        review Returns -1 if there is no review with the given identifier""" 
        if not self.valid_review(reviewId):
            return   
        review = self.find_review_line(int(reviewId))
        numerator = int.from_bytes(review[0:1], byteorder='big')
        denominator = int.from_bytes(review[1:2], byteorder='big')
        if denominator == 0 and numerator != 0:
            line = linecache.getline(os.path.join(self.index_dir, "map_review.txt"), reviewId)
            denominator = line.split()[2]
       
        return denominator

    def getReviewLength(self, reviewId):    
        """Returns the number of tokens in a given review Returns -1 if there is no review with the given identifier""" 
        if not self.valid_review(reviewId):
            return  
        review = self.find_review_line(int(reviewId))
        ReviewLength = int.from_bytes(review[3:5], byteorder='big')
        if ReviewLength:
            return ReviewLength
    
    def getProductReviews(self, productId):    
        """Return the ids of the reviews for a given product identifier Note that the integers returned should 
        be sorted by id Returns an empty Tuple if there are no reviews for this product"""
        product = self.find_line(productId, "product")
        if product:
            decode_line = self.decode_line(product, "product")
            return decode_line
        return 0
   
    def getProductReviews(self, productId):    
        """Return the ids of the reviews for a given product identifier Note that the integers returned should 
        be sorted by id Returns an empty Tuple if there are no reviews for this product"""
        product = self.find_line(productId, "product")
        if product:
            decode_line = self.decode_line(product, "product")
            return decode_line
        return 0
    
    def getProductReviews(self, productId):    
        """Return the ids of the reviews for a given product identifier Note that the integers returned should 
        be sorted by id Returns an empty Tuple if there are no reviews for this product"""
        product = self.find_line(productId, "product")
        if product:
            decode_line = self.decode_line(product, "product")
            return decode_line
        return 0      
# -------------------------------------------------------
    

def encode(number):
    """Pack `number` into varint bytes"""
    buf = b''
    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            buf += _byte(towrite | 0x80)
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
        c = stream.read(1)
        if c == b'':
            break
        i = ord(c)
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
        
