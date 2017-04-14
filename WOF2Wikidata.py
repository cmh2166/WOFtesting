import csv
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import time


def sparqlWD(label):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery("""
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?wdURI ?loc ?fast ?psh ?dmoz ?mesh ?gnd WHERE {
          ?wdURI wdt:P1566 '""" + label + """' .
          OPTIONAL { ?wdURI wdt:P244 ?loc . }
          OPTIONAL { ?wdURI wdt:P2163 ?fast . }
          OPTIONAL { ?wdURI wdt:P1051 ?psh . }
          OPTIONAL { ?wdURI wdt:P998 ?dmoz . }
          OPTIONAL { ?wdURI wdt:P486 ?mesh . }
          OPTIONAL { ?wdURI wdt:P227 ?gnd . }
        }
    """)
    sparql.setReturnFormat(JSON)
    try:
        res = sparql.query().convert()
    except:
        time.sleep(120)
        try:
            res = sparql.query().convert()
        except:
            res = None
    if len(res['results']['bindings']) > 0:
        return(res)


def main():
    WOFconc = requests.get('https://raw.githubusercontent.com/whosonfirst/whosonfirst-data/master/meta/wof-concordances-latest.csv')
    with open('WOFconcordance.csv', 'wb') as WOFcsv:
        WOFcsv.write(WOFconc.text.encode('utf-8'))
    WOFreader = csv.DictReader(open('WOFconcordance.csv'))
    rows_count = sum(1 for row in csv.reader(open('WOFconcordance.csv')))
    fn = WOFreader.fieldnames
    fn.extend(('fst:id', 'dmoz:id', 'gnd:id', 'mesh:id', 'psh:id'))
    WOFwriter = csv.DictWriter(open('WOFconcordanceUpdated.csv', 'wb'),
                               fieldnames=fn)
    WOFwriter.writeheader()
    start = added = 0
    for row in WOFreader:
        start += 1
        row['fst:id'] = row['psh:id'] = row['dmoz:id'] = ''
        row['mesh:id'] = row['gnd:id'] = ''
        resp = sparqlWD(row["gn:id"])
        time.sleep(1)
        if resp:
            if 'fast' in resp['results']['bindings'][0] and not row['fst:id']:
                row['fst:id'] = resp['results']['bindings'][0]['fast']['value']
                added += 1
            if 'dmoz' in resp['results']['bindings'][0] and not row['dmoz:id']:
                row['dmoz:id'] = resp['results']['bindings'][0]['dmoz']['value'].encode('utf-8')
                added += 1
            if 'gnd' in resp['results']['bindings'][0] and not row['gnd:id']:
                row['gnd:id'] = resp['results']['bindings'][0]['gnd']['value']
                added += 1
            if 'loc' in resp['results']['bindings'][0] and not row['loc:id']:
                row['loc:id'] = resp['results']['bindings'][0]['loc']['value']
                added += 1
            if 'mesh' in resp['results']['bindings'][0] and not row['mesh:id']:
                row['mesh:id'] = resp['results']['bindings'][0]['mesh']['value']
                added += 1
            if 'psh' in resp['results']['bindings'][0] and not row['psh:id']:
                row['psh:id'] = resp['results']['bindings'][0]['psh']['value']
                added += 1
            if 'wdURI' in resp['results']['bindings'][0] and not row['wd:id']:
                row['wd:id'] = resp['results']['bindings'][0]['wdURI']['value']
                added += 1
        WOFwriter.writerow(row)
        if start % 10 == 0:
            print(start, rows_count)
    print("Number of records: " + str(start) + " and added identifiers: " +
          str(added))


if __name__ == "__main__":
    main()
