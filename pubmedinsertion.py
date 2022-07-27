# Author: Braden Stonehill
# Course: CS 6070
# Instructor: Dr. Noh
# Date: 06/27/2022
# Program: PubMed Data Insertion for inserting xml files from PubMed into a PostgreSQL database
# Version: 0.1

import sys
import datetime
import psycopg2
from psycopg2 import OperationalError
from lxml import etree


def db_connect(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred.")
    return connection


def db_insert(connection, records):
    total_records = len(records)
    count = 0
    for record in records:
        count += 1
        sys.stdout.write(f'\rInserting record {count} of {total_records}...')
        sys.stdout.flush()
        journal_str = f'(\'{record["ISSN"]}\', \'{record["Journal Title"]}\')'
        citation_str = f'({record["PMID"]}, \'{record["ArticleDate"]}\', \'{record["Article Title"]}\', ' \
                       f'\'{record["Abstract Text"]}\', \'{record["ISSN"]}\')'

        author_str = ''
        cit_author_str = ''
        if record['Authors'] is not None:
            author_str = '\n\t'
            cit_author_str = '\n\t'
            for author in record["Authors"]:
                author_str += f'(\'{author[1]}\', \'{author[0]}\'),\n\t'
                cit_author_str += f'({record["PMID"]}, (SELECT authorid FROM author WHERE fore_name=\'{author[1]}\' AND last_name=\'{author[0]}\' LIMIT 1)),\n\t'
            author_str = author_str[:-3]
            cit_author_str = cit_author_str[:-3] + ';'

        kw_str = ''
        cit_kw_str = ''
        if record['Keywords'] is not None:
            kw_str = '\n\t'
            cit_kw_str = '\n\t'
            for keyword in record['Keywords']:
                kw_str += f'(\'{keyword}\'),\n\t'
                cit_kw_str += f'({record["PMID"]}, (SELECT kwid FROM keyword WHERE term=\'{keyword}\' LIMIT 1)),\n\t'
            kw_str = kw_str[:-3]
            cit_kw_str = cit_kw_str[:-3] + ';'

        mesh_str = ''
        cit_mesh_str = ''
        if record['MeSH codes'] is not None:
            mesh_str = '\n\t'
            cit_mesh_str = '\n\t'
            for codes in record['MeSH codes']:
                mesh_str += f'(\'{codes[0]}\', \'{codes[1]}\'),\n\t'
                cit_mesh_str += f'({record["PMID"]}, \'{codes[0]}\'),\n\t'
            mesh_str = mesh_str[:-3]
            cit_mesh_str = cit_mesh_str[:-3] + ';'

        journal_qry = 'INSERT INTO journal (issn, journal_title) VALUES ' + journal_str + ' ON CONFLICT DO NOTHING;'

        citation_qry = 'INSERT INTO citation (pmid, date_completed, article_title, abstract_text, issn) VALUES ' + \
                       citation_str + ' ON CONFLICT DO NOTHING;'

        author_qry = 'INSERT INTO author (fore_name, last_name) VALUES ' + author_str + ' ON CONFLICT DO NOTHING;' \
            if record['Authors'] is not None else ''

        cit_author_qry = 'INSERT INTO cit_author (pmid, authorid) VALUES ' + cit_author_str \
            if record['Authors'] is not None else ''

        mesh_qry = 'INSERT INTO mesh (meshid, term) VALUES ' + mesh_str + ' ON CONFLICT DO NOTHING;' \
            if record['MeSH codes'] is not None else ''

        cit_mesh_qry = 'INSERT INTO cit_mesh (pmid, meshid) VALUES ' + cit_mesh_str \
            if record['MeSH codes'] is not None else ''

        kw_qry = 'INSERT INTO keyword (term) VALUES ' + kw_str + ' ON CONFLICT DO NOTHING;' \
            if record['Keywords'] is not None else ''

        cit_kw_qry = 'INSERT INTO cit_keyword (pmid, kwid) VALUES ' + cit_kw_str \
            if record['Keywords'] is not None else ''

        input_qry = (
            f'{journal_qry} {citation_qry} {author_qry} {cit_author_qry} '
            f'{mesh_qry} {cit_mesh_qry} {kw_qry} {cit_kw_qry}'
        )

        try:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(input_qry)
        except Exception as e:
            message = str(e)
            print(e)
            print(input_qry)

    connection.close()


def parse_xml(filename):
    with open(filename, 'r') as f:
        contents = f.readlines()

    contents[1] = contents[1][:10] + 'SET' + contents[1][26:]
    contents.insert(2, '<SET>')
    contents.append('</SET>')

    with open(filename[:-4] + 'edit.xml', 'w') as f:
        contents = ''.join(contents)
        f.write(contents)

    xml_tree = etree.parse(filename[:-4] + 'edit.xml')
    root = xml_tree.getroot()
    citation_filter = [
        'PMID',
        'DateCompleted',
        'Article',
        'MeshHeadingList',
        'KeywordList'
    ]
    records = list()
    total_sets = len(root)
    set_count = 0
    for article_set in root:
        set_count += 1
        total_articles = len(article_set)
        count = 0
        for article in article_set:
            count += 1
            sys.stdout.write(f'\rProcessing article {count} of {total_articles}...')
            sys.stdout.flush()
            record = {
                'PMID': '',
                'ArticleDate': None,
                'Article Title': '',
                'Abstract Text': '',
                'MeSH codes': None,
                'Keywords': None,
                'Authors': None,
                'Journal Title': '',
                'ISSN': ''
            }
            citation = article[0]
            for element in citation.iter(citation_filter):
                if element.tag == 'PMID':
                    record['PMID'] = element.text
                elif element.tag == 'DateCompleted':
                    record['ArticleDate'] = f'{element[0].text}-{element[1].text}-{element[2].text}'
                elif element.tag == 'Article':
                    article_filter = [
                        'Journal',
                        'ArticleDate',
                        'ArticleTitle',
                        'Abstract',
                        'AuthorList',
                    ]
                    for child in element.iter(article_filter):
                        if child.tag == 'Journal':
                            record['ISSN'] = child[0].text
                            if child[2].text is not None:
                                record['Journal Title'] = child[2].text.replace("'", "''")
                                if len(record['Journal Title']) > 256:
                                    record['Journal Title'] = record['Journal Title'][:256]
                        elif child.tag == 'ArticleTitle':
                            if child.text is not None:
                                record['Article Title'] = child.text.replace("'", "''")
                                if len(record['Article Title']) > 256:
                                    record['Article Title'] = record['Article Title'][:256]
                        elif child.tag == 'Abstract':
                            if child[0].text is not None:
                                record['Abstract Text'] = child[0].text.replace("'", "''")
                        elif child.tag == 'AuthorList':
                            authors = list()
                            for author in child:
                                ln = ''
                                fn = ''
                                for name in author.iter('LastName', 'ForeName'):
                                    if name.tag == 'LastName':
                                        ln = name.text.replace("'", "''")
                                    elif name.tag == 'ForeName':
                                        fn = name.text.replace("'", "''")
                                authors.append((ln, fn))
                            record['Authors'] = authors
                        elif child.tag == 'ArticleDate' and record['ArticleDate'] is None:
                            record['ArticleDate'] = f'{child[0].text}-{child[1].text}-{child[2].text}'
                elif element.tag == 'MeshHeadingList':
                    codes = set()
                    for mesh in element:
                        for code in mesh:
                            codes.add((code.get('UI'), code.text.replace("'", "''")))
                    record['MeSH codes'] = codes
                elif element.tag == 'KeywordList':
                    keywords = set()
                    for keyword in element:
                        keywords.add(keyword.text.replace("'", "''"))
                    record['Keywords'] = keywords
            if record['ArticleDate'] is None:
                pubdata = article[1]
                for child in pubdata[0]:
                    if child.get('PubStatus') == 'pubmed':
                        record['ArticleDate'] = f'{child[0].text}-{child[1].text}-{child[2].text}'
            records.append(record)
        print('')
    print(f'\rFinished set {set_count} of {total_sets}')
    return records


if __name__ == '__main__':
    _, db_name, db_user, db_password, db_host, db_port, db_filepath = sys.argv
    connection = db_connect(db_name, db_user, db_password, db_host, db_port)
    records = parse_xml(db_filepath)
    db_insert(connection, records)