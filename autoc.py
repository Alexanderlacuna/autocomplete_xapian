import xapian
import csv  
import json 



NGRAM_MIN_LENGTH = 4
NGRAM_MAX_LENGTH = 15


from flask import Flask

app = Flask(__name__)


class SearchQuerySet:
    pass   

def to_xapian_term(term):
    """
    Converts a Python type to a
    Xapian term that can be indexed.
    """
    return str(term).lower()



# indexing part 
def get_ngram_lengths(value):
    values = value.split()

    for item in values:
        for ngram_length  in range(NGRAM_MIN_LENGTH,NGRAM_MAX_LENGTH+1):
            yield item,ngram_length

    # todo

    """

    for obj in iterable:
        document =  xapian.Document()
        term_generator.set_document(document)


"""



def ngram_terms(value):
    for item,length in  get_ngram_lengths(value):
        item_length = len(item)

        for start in range(0,item_length-length+1):
            for size in range(length,length+1):
                end = start + size

                if end > item_length:
                    continue

                yield _to_xapian_term(item[start:end])

def  edge_ngram_terms(value):
    for item,length in get_ngram_lengths(value):
        yield  to_xapian_term(item[0:length])





def add_edge_ngram_to_document(document,prefix, value, weight):
    # split term in ngram add each ngram to the index
    # the minimum_min_length<=ngram=> ngram_max_lenght
    for term in edge_ngram_terms(value):
        document.add_term(term, weight)
        document.add_term(prefix+term,weight)

def parse_csv_file(datapath, charset='utf8'):
    """Parse a CSV file.
    Assumes the first row has field names.
    Yields a dict keyed by field name for the remaining rows.
    """
    with open(datapath) as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            yield row




def index(datapath, dbpath):
    # create or open te database for writing

    db = xapian.WritableDatabase(dbpath,xapian.DB_CREATE_OR_OPEN)

    # set up a term generator that we''ll use in indexing

    termgenerator = xapian.TermGenerator()

    termgenerator.set_stemmer(xapian.Stem("en"))


    for fields in parse_csv_file(datapath):
        # field is a dictionary mapping from from field name to value

        # pick out the field we intend to index  here
        description = fields.get('DESCRIPTION', u'')
        title = fields.get('TITLE', u'')
        identifier = fields.get('id_NUMBER', u'')

        # we  make a document and tell the term generator to use it 

        doc = xapian.Document()
        termgenerator.set_document(doc)


        # indexing step ;;indexing each with suitable prefix
        termgenerator.index_text(title, 1, 'S')
        termgenerator.index_text(description, 1, 'XD')


        # Index fields without prefixes for general search.
        termgenerator.index_text(title)
        termgenerator.increase_termpos()
        termgenerator.index_text(description)


        add_edge_ngram_to_document(doc,"EG", title, 1)
        add_edge_ngram_to_document(doc,"EG", description, 1)

        # index  the material field by semicolon


        for material in fields.get("MATERIALS",u"").split(";"):
            material = material.strip().lower()

            if len(material) > 0:
                doc.add_boolean_term("XM" + material)

        ##3 end of new indexing code


        # Store all the fields for display purposes.
        doc.set_data(json.dumps(fields))


        # We use the identifier to ensure each object ends up in the
        # database only once no matter how many times we run the
        # indexer.
        idterm = u"Q" + identifier
        doc.add_boolean_term(idterm)
        db.replace_document(idterm, doc)


# this is the step for working on the searchxs

def search(dbpath,querystring,offset=0,pagesize=10):
    # offset -> defines starting point within result set

    # pagesize -  define the number of records to retrieve

    # open the database we're going to searchg


    db = xapian.Database(dbpath)

    # set up a queryparser with a stermmner and suittable prefixes


    queryparser = xapian.QueryParser()
    queryparser.set_stemmer(xapian.Stem('en'))
    queryparser.set_stemming_strategy(queryparser.STEM_SOME)

    # start of prefix configuration
    queryparser.add_prefix("title","S")
    queryparser.add_prefix("description","XD")
    queryparser.add_prefix("hits","EG")
    # end of prefix configuration


    # add parse the query


    #queryparser.add_prefix("hits","EDGE")

    query = queryparser.parse_query(querystring)


    """


    if len(materials) > 0:


        # filter results to one which  contain atleast 

        # materials


        # build a query for each material value

        material_queries = [
        xapian.Query("XM" + material.lower())
        for material in materials
        ]

        # combiner these queries with or operator

        material_query = xapian.Query(xapian.Query.OP_OR, material_queries)

        # use the material query to query main query

        query = xapian.Query(xapian.Query.OP_FILTER, query, material_query)


    """


    # Use an Enquire object on the database to run the query
    enquire = xapian.Enquire(db)
    enquire.set_query(query)

    #breakpoint()


    # add print out match in each

    matches = []

    for match in enquire.get_mset(offset, pagesize):

        fields =  json.loads(match.document.get_data())

        breakpoint()

        print(f"{match.rank+1} {match.docid} {fields.get('TITLE',u'')}")

        matches.append(match.docid)

  



"""
to perform search take query object and  perform bit manipulation and && or 
"""


def autocomplete_search(db_path,auto_word,offset=0,pagesize=10):
    db = xapian.Database(db_path)
    queryparser = xapian.QueryParser()
    queryparser.set_stemmer(xapian.Stem('en'))
    queryparser.set_stemming_strategy(queryparser.STEM_SOME)

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print(auto_word)

    queryparser.add_prefix("hits","EG")


    queryparser.add_prefix("title","S")
    queryparser.add_prefix("description","XD")
    query = queryparser.parse_query(auto_word)
    breakpoint()

    enquire = xapian.Enquire(db)
    enquire.set_query(query)


    for match in enquire.get_mset(offset,pagesize):
        fields = json.loads(match.document.get_data())

        breakpoint()

        print(f"{match.rank+1} {match.docid} {fields.get('TITLE',u'')}")

        matches.append(match.docid)



def run():
    import sys  

    file_name,db_name = sys.argv[1],sys.argv[2]

    index(file_name, db_name)


#run()


def search2(dbpath, querystring, offset=0, pagesize=10):
    # offset - defines starting point within result set
    # pagesize - defines number of records to retrieve

    # Open the database we're going to search.
    db = xapian.Database(dbpath)

    # Set up a QueryParser with a stemmer and suitable prefixes
    queryparser = xapian.QueryParser()
    queryparser.set_stemmer(xapian.Stem("en"))
    queryparser.set_stemming_strategy(queryparser.STEM_SOME)
    # Start of prefix configuration.
    queryparser.add_prefix("title", "S")
    queryparser.add_prefix("description", "XD")
    # End of prefix configuration.

    # And parse the query
    query = queryparser.parse_query(querystring)



    # Use an Enquire object on the database to run the query
    enquire = xapian.Enquire(db)
    enquire.set_query(query)

    # And print out something about each match
    matches = []
    for match in enquire.get_mset(offset, pagesize):

        fields = json.loads(match.document.get_data())

        breakpoint()
        print(u"%(rank)i: #%(docid)3.3i %(title)s" % {
            'rank': match.rank + 1,
            'docid': match.docid,
            'title': fields.get('TITLE', u''),
            })
        matches.append(match.docid)

    # Finally, make sure we log the query and displayed results
# below is searching for separate fields


#autocomplete_search("/tmp/xapian","moun")


#run()
#results = search("/tmp/xapian","hits:1659")


#results3 = search("/tmp/xapian", "application",[])


#print(results3)


#print(results3)

#print(results)