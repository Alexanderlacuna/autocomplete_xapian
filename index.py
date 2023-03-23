import json
import xapian



NGRAM_MIN_LENGTH = 4
NGRAM_MAX_LENGTH = 15








from flask import Flask

app = Flask(__name__)



def index(datapath, dbpath):

	# docs
	db = xapian.WritableDatabase(dbpath, xapian.DB_CREATE_OR_OPEN)

	# termgenenrator

	term_generator = xapian.TermGenerator()
	term_generator.set_stemmer(xapian.Stem("en"))

	# data you need to index in certain format assume csv for now

	for fields in parse_csv_file(datapath):
		# field dict mapping name-> value
		# pick out the field needed to index
		description - field.get("DESCRIPTION", u"")
		title = field.get("TITLE", u"")
		identifier = field.get("id_NUMBER", u"")

	    # make a document add tell the term generator to use

	    doc  = xapian.Document()

	    term_generator.set_document(doc)
	    # index field with suitable prefxi ;;from example

	    term_generator.index_text(title,1,"S")

	    term_generator.index_text(description,1, "XD")



	    # index field without prefixes for search

	    term_generator.index_title(title)

	    term_generator.increase_termpos(
	    	)

	    term_generator.index(description)


	    # store field for displat 

	    doc.set_data(json.dumps(fields))

	    idterm = u"Q" + identifier

	    doc.add_boolean_term(idterm)

	    db.replace_document(iditerm,doc)



# indexing part 
def get_ngram_lengths(lengths):
	values = value.split()

	for item in values:
		for ngram_length  in range(NGRAM_MIN_LENGTH,NGRAM_MAX_LENGTH+1):
			yield item,ngram_length

	# todo

	for obj in iterable:
		document =  xapian.Document()
		term_generator.set_document(document)


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


def autocomplete(request):

	# example of expected results 
	# 
	sqs = SearchQuerySet().autocomplete(content_auto=request.GET.get('q',""))[:5]

	suggestions = [result.title for result in sqs]

	return (json.dumps({
		'results':suggestions
		}))





# multfield search  pass files and search term

#https://stackoverflow.com/questions/12262590/django-haystack-autocompletion-on-two-multiple-fields

class MySearchQuerySet(SearchQuerySet):
    def mfac(self, fields, fragment, **kwargs):
        """
        Multi-field checks for autocomplete.
        `fields` : Iterable composed of field names to search against
        `fragment`: The term sought
        
        """
        clone = self._clone()
        query_bits = []
        lookup = kwargs.get('lookup', '__contains')

        for field in fields:
            subqueries = []
            for word in fragment.split(' '):
                kwargs = {field + lookup: Clean(word)}
                subqueries.append(SQ(**kwargs))
            query_bits.append(reduce(operator.and_, subqueries))

        return clone.filter(reduce(operator.or_, query_bits))


@app.route('/search/autocomplete', methods=['POST']) 
def foo():
    query = request.json

    return ""




    return jsonify(data)