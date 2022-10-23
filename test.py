# Author Joey Whelan

from redis import Connection, from_url
from redis.commands.search.field import TagField, TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import random, string
from time import sleep
from enum import Enum
from argparse import ArgumentParser, ArgumentTypeError
import pandas as pd

REDIS_URL: str = 'redis://localhost:6379'       # default Redis connect string
NUM_KEYS: int = 10000                           # default total keys to be produced for the index
NUM_FIELDS: int = 10                            # default number of fields per key
TEXT_FIELD_SIZE: int = 10                       # default number of characters in each text field
NUMERIC_FIELD_SIZE: int = 10                    # default number of digits in each numeric field
FORMATS: list = ['text', 'html', 'markdown']    # output format types.  default is text.

# Enumerated types for the various params
class OBJECT_TYPE(Enum):
    HASH = 1
    JSON = 2

class FIELD_TYPE(Enum):
    NUMERIC = 1
    TAG = 2
    TEXT = 3

class DATA_TYPE(Enum):
    NUMBER = 1
    STRING = 2

class TestSuite(object):
    """ Main class for instantiating the index tests
    """

    # class constants for Redis object naming
    IDX_NAME: str   = 'idx'   
    KEY_NAME: str   = 'key'
    FIELD_NAME: str = 'field'

    def __init__(self, args: dict): 

        # instance vars
        self.r: Connection              = from_url(args.url)
        self.num_keys: int              = args.nkeys
        self.num_fields: int            = args.nfields
        self.text_field_size: int       = args.textsize
        self.numeric_field_size: int    = args.numericsize
        self.format: str                = args.format

    def test_all(self) -> pd.DataFrame:
        """ Public function for initiating all index tests.

            Returns
            -------
            Pandas Dataframe with all the test results
        """
        results: dict = {}
        
        #hash sets with string fields
        print()
        print('Hash Sets with Strings tests')
        self.r.flushdb() 
        self._load_db(OBJECT_TYPE.HASH, DATA_TYPE.STRING)
        results['Hash Text Unsorted'] = self._test(OBJECT_TYPE.HASH, FIELD_TYPE.TEXT, False)
        print(f'Test 1 - Hash Text Unsorted: {results["Hash Text Unsorted"]}')
        results['Hash Text Sorted'] = self._test(OBJECT_TYPE.HASH, FIELD_TYPE.TEXT, True)
        print(f'Test 2 - Hash Text Sorted: {results["Hash Text Sorted"]}')
        results['Hash Tag'] = self._test(OBJECT_TYPE.HASH, FIELD_TYPE.TAG, False)
        print(f'Test 3 - Hash Tag: {results["Hash Tag"]}')

        #hash sets with numeric fields
        print()
        print('Hash Sets with Numerics tests')
        self.r.flushdb() 
        self._load_db(OBJECT_TYPE.HASH, DATA_TYPE.NUMBER)
        results['Hash Numeric Unsorted'] = self._test(OBJECT_TYPE.HASH, FIELD_TYPE.NUMERIC, False)
        print(f'Test 4 - Hash Numeric Unsorted: {results["Hash Numeric Unsorted"]}')
        results['Hash Numeric Sorted'] = self._test(OBJECT_TYPE.HASH, FIELD_TYPE.NUMERIC, True)
        print(f'Test 5 - Hash Numeric Sorted: {results["Hash Numeric Sorted"]}')
        
        #json with string fields
        print()
        print('JSON with Strings tests')
        self.r.flushdb() 
        self._load_db(OBJECT_TYPE.JSON, DATA_TYPE.STRING)
        results['JSON Text Unsorted'] = self._test(OBJECT_TYPE.JSON, FIELD_TYPE.TEXT, False)
        print(f'Test 6 - JSON Text Unsorted: {results["JSON Text Unsorted"]}')
        results['JSON Text Sorted'] = self._test(OBJECT_TYPE.JSON, FIELD_TYPE.TEXT, True)
        print(f'Test 7 - JSON Text Sorted: {results["JSON Text Sorted"]}')
        results['JSON Tag'] = self._test(OBJECT_TYPE.JSON, FIELD_TYPE.TAG, False)
        print(f'Test 8 - JSON Tag: {results["JSON Tag"]}')
        
        #json with numeric fields
        print()
        print('JSON with Numerics tests')
        self.r.flushdb() 
        self._load_db(OBJECT_TYPE.JSON, DATA_TYPE.NUMBER)
        results['JSON Numeric Unsorted'] = self._test(OBJECT_TYPE.JSON, FIELD_TYPE.NUMERIC, False)
        print(f'Test 9 - JSON Numeric Unsorted: {results["JSON Numeric Unsorted"]}')
        results['JSON Numeric Sorted'] = self._test(OBJECT_TYPE.JSON, FIELD_TYPE.NUMERIC, True)
        print(f'Test 10 - JSON Numeric Sorted: {results["JSON Numeric Sorted"]}')
        
        df = pd.DataFrame(results).T
        df.index.name = 'Index Structure'
        match args.format:
            case 'text':
                return df
            case 'html':
                return df.to_html()
            case 'markdown':
                return df.to_markdown()
 
    def _test(self, object_type: OBJECT_TYPE, field_type: FIELD_TYPE, sorted: bool) -> dict:
        """ Private function for initiating a single test.
            Parameters
            ----------
            object_type - enum signifying the Redis object type: hash or json
            field_type - enum signifying the Redis field type: text, tag, or numeric
            sorted - boolean flag for a sorted index

            Returns
            -------
            Pandas Dataframe with all the test results
        """
        idx_def: IndexDefinition
        field_prefix: str
        match object_type:
            case OBJECT_TYPE.HASH:
                idx_def = IndexDefinition(index_type=IndexType.HASH, prefix=[f'{TestSuite.KEY_NAME}:'])
                field_prefix = f'{TestSuite.FIELD_NAME}'
            case OBJECT_TYPE.JSON:
                idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=[f'{TestSuite.KEY_NAME}:'])
                field_prefix = f'$.{TestSuite.FIELD_NAME}'

        schema: list = list()
        for j in range(self.num_fields):
            match field_type:
                case FIELD_TYPE.NUMERIC:
                    schema.append(NumericField(f'{field_prefix}_{j}', as_name=f'{TestSuite.FIELD_NAME}_{j}', sortable=sorted))
                case FIELD_TYPE.TAG:
                    schema.append(TagField(f'{field_prefix}_{j}', as_name=f'{TestSuite.FIELD_NAME}_{j}'))
                case FIELD_TYPE.TEXT:
                    schema.append(TextField(f'{field_prefix}_{j}', as_name=f'{TestSuite.FIELD_NAME}_{j}', sortable=sorted))
        
        self.r.ft(TestSuite.IDX_NAME).create_index(schema, definition=idx_def)
        stats = self._get_stats()
        self.r.ft(TestSuite.IDX_NAME).dropindex()
        return stats

    def _get_stats(self) -> dict:
        """ Private function for extracting memory footprint stats from Redis
            Parameters
            ----------
            none
            
            Returns
            -------
            dict with test results
        """

        info: dict = self.r.ft(TestSuite.IDX_NAME).info()
        while (float(info['percent_indexed']) < 1):
            sleep(1)
            info = self.r.ft(TestSuite.IDX_NAME).info()

        stats: dict = {}
        stats['Object Size(b)'] = self.r.memory_usage(f'{TestSuite.KEY_NAME}:0')
        stats['Index Size(mb)'] = round(float(info['inverted_sz_mb']) + \
            float(info['vector_index_sz_mb']) + \
            float(info['offset_vectors_sz_mb']) + \
            float(info['doc_table_size_mb']) + \
            float(info['sortable_values_size_mb']) + \
            float(info['key_table_size_mb']), 2) 
        return stats

    def _rand_str(self) -> str:
        """ Private function for generating a random string of specified length
            Parameters
            ----------
            none
            
            Returns
            -------
            random alpha string
        """
        return ''.join(random.choices(string.ascii_letters, k=self.text_field_size))

    def _rand_num(self) -> int:
        """ Private function for generating a random set of digits of specified length
            Parameters
            ----------
            none
            
            Returns
            -------
            random digits
        """
        return int(''.join(random.choices(string.digits, k=self.numeric_field_size)))


    def _load_db(self, object_type: OBJECT_TYPE, data_type: DATA_TYPE) -> None:
        """ Private function for loading data into Redis for a test
            Parameters
            ----------
            object_type - enum signifying the Redis object type: hash or json
            data_type - enum signifying the data type for the field:  string or number

            Returns
            -------
            none
        """
        pipe = self.r.pipeline()
        for i in range(self.num_keys): 
            fields: dict = {}
            for j in range(self.num_fields):
                match data_type:
                    case DATA_TYPE.NUMBER:
                        fields[f'field_{j}'] = self._rand_num()
                    case DATA_TYPE.STRING:
                        fields[f'field_{j}'] = self._rand_str()
            match object_type:
                case OBJECT_TYPE.HASH:
                    pipe.hset(f'key:{i}', mapping=fields)
                case OBJECT_TYPE.JSON:
                    pipe.json().set(f'key:{i}', '$', fields)
        pipe.execute()

def check_nkeys_arg(value: str) -> int:
    """ Arg parser validation of num keys param
            Parameters
            ----------
            value - string representing number of keys to be produced for the test

            Returns
            -------
            int - input value casted to int
    """
    ival: int = int(value)
    if ival < 1 or ival > 1000000:
        raise ArgumentTypeError('number of keys must be between 1 and 1,000,000')
    return ival

def check_nfields_arg(value: str) -> int:
    """ Arg parser validation of num fields param
            Parameters
            ----------
            value - string representing number of fields per key for the test

            Returns
            -------
            int - input value casted to int
    """
    ival: int = int(value)
    if ival < 1 or ival > 1000:
        raise ArgumentTypeError('number of fields must be between 1 and 1000')
    return ival

def check_text_size_arg(value: str) -> int:
    """ Arg parser validation of text size param
            Parameters
            ----------
            value - string representing number of characters for text fields

            Returns
            -------
            int - input value casted to int
    """
    ival: int = int(value)
    if ival < 1 or ival > 1000:
        raise ArgumentTypeError('size of fields must be between 1 and 1000')
    return ival

def check_numeric_size_arg(value: str) -> int:
    """ Arg parser validation of numeric size param
            Parameters
            ----------
            value - string representing number of digits for numeric fields

            Returns
            -------
            int - input value casted to int
    """
    ival: int = int(value)
    if ival < 1 or ival > 15:
        raise ArgumentTypeError('size of fields must be between 1 and 15')
    return ival

if __name__ == '__main__':
    parser = ArgumentParser(description='Redis Document memory footprint comparator')
    parser.add_argument('--url', required=False, type=str, default=REDIS_URL,
        help='Redis URL connect string')
    parser.add_argument('--nkeys', required=False, type=check_nkeys_arg, default=NUM_KEYS,
        metavar="[1,1000000]",  help='Number of unique keys to be created and indexed')
    parser.add_argument('--nfields', required=False, type=check_nfields_arg, default=NUM_FIELDS,
        metavar="[1,1000]", help='Number of fields per key')
    parser.add_argument('--textsize', required=False, type=check_text_size_arg, default=TEXT_FIELD_SIZE,
        metavar="[1,1000]", help='Size (characters) of text fields')
    parser.add_argument('--numericsize', required=False, type=check_numeric_size_arg, default=NUMERIC_FIELD_SIZE,
        metavar="[1,15]", help='Size (characters) of numeric fields')
    parser.add_argument('--format', required=False, choices=FORMATS, default='text',
        help='Output format.  Valid inputs:  text, html, markdown')
    args = parser.parse_args()
    
    test = TestSuite(args)
    results = test.test_all()
    print()
    print(f'Consolidated Results - Num Keys:{args.nkeys}, Num Fields:{args.nfields}, Text Field Size:{args.textsize}, Numeric Field Size:{args.numericsize}')
    print(results)