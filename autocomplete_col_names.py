from xapian import Database, Query, QueryParser, TermGenerator


import csv
import xapian

def index_autocomplete_data(db_path, csv_path):
    """
    Indexes the data from a CSV file for autocomplete using Xapian.
    """
    # Set up the database
    db = xapian.WritableDatabase(db_path, xapian.DB_CREATE_OR_OPEN)
    stemmer = xapian.Stem("en")
    prefix_len = 3
    indexers = []

    # Set up the indexers for each column
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        column_names = next(reader)
        for column_name in column_names:
            indexer = xapian.TermGenerator()
            indexer.set_stemmer(stemmer)
            indexer.set_database(db)
            indexer.set_flags(xapian.TermGenerator.FLAG_SPELLING)
            indexer.set_stemming_strategy(xapian.TermGenerator.STEM_SOME)
            indexers.append(indexer)

        # Index the data
        for row in reader:
            doc = xapian.Document()
            for i, value in enumerate(row):
                indexer = indexers[i]
                indexer.set_document(doc)
                indexer.index_text(value, 1, column_names[i])
            db.add_document(doc)


from xapian import Database, Query, QueryParser, TermGenerator
from xapian import Database, Query, QueryParser, TermGenerator

from xapian import Database, Query, QueryParser, TermGenerator

def autocomplete(db_path, query_str, max_results=10):
    # Open the database
    db = Database(db_path)

    # Set up the query parser and term generator
    qp = QueryParser()
    tg = TermGenerator()
    qp.set_database(db)
    qp.set_stemming_strategy(QueryParser.STEM_NONE)

    # Parse the query string and generate prefixes
    query = qp.parse_query(query_str)
    prefixes = set()

    breakpoint()

    for term in qp.get_term(query):
        tg.set_content(term.decode())
        prefix_len = min(len(term), 3)
        for prefix in tg.generate_prefixes(prefix_len):
            prefixes.add(prefix)

    # Perform the search and retrieve the matching documents
    enquire = Query(db).filter(Query(Query.OP_OR, [Query('S' + prefix) for prefix in prefixes]))
    matches = enquire.get_mset(0, max_results)

    # Extract the data for the matching documents
    data = []
    for match in matches:
        doc = db.get_document(match.docid)
        data.append({field.name: doc.get_value(field) for field in db.get_metadata()})
    return data


# Sample data in CSV format
csv_data = """Name,Email,Phone
John Doe,john@example.com,555-1234
Jane Smith,jane@example.com,555-5678
Bob Johnson,bob@example.com,555-9101"""

# Write sample data to a file
with open('data.csv', 'w') as f:
    f.write(csv_data)

# Index the data for autocomplete
index_autocomplete_data('/tmp/xapian', 'data.csv')

# Perform an autocomplete search
completions = autocomplete('/tmp/xapian', 'joh')
#print(completions)
