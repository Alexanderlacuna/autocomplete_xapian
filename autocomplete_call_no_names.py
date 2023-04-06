import xapian

def index_documents(documents):
    # Create or open the database
    database_path = "./autocomplete_db"
    database = xapian.WritableDatabase(database_path, xapian.DB_CREATE_OR_OPEN)

    # Create a term generator and set it up for generating n-grams
    term_generator = xapian.TermGenerator()
    term_generator.set_stemmer(xapian.Stem("en"))
    term_generator.set_stemming_strategy(xapian.TermGenerator.STEM_SOME)
    term_generator.set_word_length(2)
    term_generator.set_max_word_length(15)

    # Create a document object and add fields to it
    for doc_id, doc_data in enumerate(documents):
        document = xapian.Document()

        # Index all fields in the document
        for field_name, field_value in doc_data.items():
            document.add_value(doc_id, field_value)
            term_generator.set_document(document)

            # Index the field value using n-grams
            term_generator.index_text(field_value)

        # Set the document id and add it to the database
        document.set_data(str(doc_id))
        database.add_document(document)

    database.flush()

def search_autocomplete(query_string, database_path, offset=0, pagesize=10):
    # Open the database
    database = xapian.Database(database_path)

    # Create a query parser and set it up for prefix searches
    query_parser = xapian.QueryParser()
    query_parser.set_stemmer(xapian.Stem("en"))
    query_parser.set_stemming_strategy(xapian.QueryParser.STEM_SOME)
    query_parser.set_default_op(xapian.Query.OP_OR)
    query_parser.set_max_wildcard_expansion(100)
    query_parser.set_default_wildcard_flags(xapian.QueryParser.WILDCARD_MATCH_ALL)
    query_parser.add_prefix("", "")
    query_parser.add_boolean_prefix("", "")

    # Parse the query string and generate n-grams for autocomplete
    query = query_parser.parse_query(query_string)
    term_generator = xapian.TermGenerator()
    term_generator.set_stemmer(xapian.Stem("en"))
    term_generator.set_stemming_strategy(xapian.TermGenerator.STEM_SOME)
    term_generator.set_word_length(2)
    term_generator.set_max_word_length(15)
    term_generator.set_document(xapian.Document())
    term_generator.set_flags(xapian.TermGenerator.FLAG_SPELLING)

    # Generate n-grams for autocomplete
    for term in query.terms():
        term_generator.set_term(term.term)
        for prefix in term_generator.generate_all_prefixes():
            query.add_prefix(prefix)

    # Set up the enquire object and sort by relevance
    enquire = xapian.Enquire(database)
    enquire.set_query(query)
    enquire.set_sort_by_value(0, True)

    # Retrieve and return the matching documents
    matches = enquire.get_mset(offset, pagesize)
    return [database.get_document(match.docid).get_data() for match in matches]
