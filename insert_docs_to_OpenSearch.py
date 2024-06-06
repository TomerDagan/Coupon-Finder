from botocore.credentials import Credentials
from write_to_log import write_to_log
import sys
import urllib3
from opensearchpy.helpers import bulk
import sys
import json
from OpenSearch import create_OpenSearch_client

def count_documents(client, index_name='deals'):
    try:
        # Get the document count
        response = client.count(index=index_name)   
        # Extract the count from the response
        doc_count = response['count']
        write_to_log('INFO', f"Number of documents in the index {index_name}: {doc_count}", 'insert_docs_to_OpenSearch.py', 'count_documents')
    except Exception as e:
        write_to_log('Error', f'Failed to count documents in index {index_name}: {str(e)}', 'insert_docs_to_OpenSearch.py', 'count_documents')

def create_index_deals(index_name,client):

    index_body = {
    'settings': {
        'index': {
            'number_of_shards': 1
        }
    },
    'mappings': {
        'properties': {
            "deal_id": {
                "type": "keyword"
            },
            "other_fields": {
                "properties": {
                    "deal": {"type": "text"},
                    "price": {"type": "text"},
                    "comments": {"type": "text"},
                    "link_for_purchase": {"type": "text"},
                    "source": {"type": "text"},
                    "is_starting_from": {"type": "text"},
                    "deal_for_day": {"type": "text"},
                    "Expire Date": {"type": "text"},
                    "extraction_date": {"type": "text"}
                }
            }
        }
      }
    }

    response = client.indices.create(index_name, body=index_body)
    print('\nCreating index:')
    print(response)

def delete_all_doc(client,index_name):
    query = {
        "query": {
            "match_all": {}
        }
    }
    try:
        response = client.delete_by_query(index=index_name, body=query)
        if ("result" in response and response["result"] == "deleted") or (response['total']==response['deleted']):
            print("All documents deleted successfully.")
            write_to_log('Info', f'All documents deleted successfully from index {index_name}', 'insert_docs_to_OpenSearch.py', 'delete_all_doc')
        else:
            print("Failed to delete documents.")
            write_to_log('Error', f'Failed to delete documents from index {index_name}: {response}', 'insert_docs_to_OpenSearch.py', 'delete_all_doc')
    except Exception as e:
        write_to_log('Error', f'{e}', 'insert_docs_to_OpenSearch.py', 'delete_all_doc')


def insert_bulk_documents(docs, index_name, client):    
    actions = []
    try:
        for doc in docs:
            action = {
                "_index": index_name,
                "_id": doc["deal_id"],
                "_source": doc
            }
            actions.append(action)
        success, bulk_result = bulk(client, actions, index=index_name)
    except Exception as e:
        write_to_log('Error', f'{e} - {bulk_result}, index={index_name}', 'insert_docs_to_OpenSearch.py', 'insert_bulk_documents')
    write_to_log('Info', f'{success} - for bulk, {len(docs)} documents been inserted to OpenSearch DB, index={index_name}', 'insert_docs_to_OpenSearch.py', 'insert_bulk_documents')
    return success
  
def delete_deal(client, index_name, deal_id):
    try:
        response = client.delete(
            index=index_name,
            id=deal_id
        )
        print('\nDeleting document:')
        print(response)
        count_documents(client,index_name)
    except Exception as e:
        print(f"Failed to delete document with ID {deal_id}: {e}")

def search_word(client,index_name,word):
    query = {
    "query": {
        "wildcard": {
            "deal": f'*{word}*'
        }
    }
    }     
    response = client.search(
            body = query,
            index = index_name
        )
    for hit in response['hits']['hits']:
            # Access the document source
            document = hit['_source']
            print(document)
            #deal_name = document.get('deal', 'N/A')
            #print(f"Deal: {deal_name}")
    
def search_word_for_count(client,index_name,word=None):
    query = {
    "query": {
        "wildcard": {
            "deal": f'*'
        }
    }
    }          

    response = client.search(
        body = query,
        index = index_name
    )
    counter=0
    for hit in response['hits']['hits']:
        hit
        counter+=1
    write_to_log('INFO', f"Number of documents in the index {index_name}: {counter}", 'insert_docs_to_OpenSearch.py', 'count_documents')

def connect_to_OpenSearch_and_add_deals():

    sys.path.append('/opt/airflow/dags/Scripts')
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    client=create_OpenSearch_client()
    print(f'client:{client}')
    write_to_log('Info', 'sucessfully created Open Search Client', 'OpenSearch.py', 'create_OpenSearch_client')
    #write_to_log('Info', 'Connection successfully made to OpenSearch domain', 'insert_docs_to_OpenSearch.py', 'create_OpenSearch_client')
    #write_to_log('Error', 'Failed to create Open Search Client', 'insert_docs_to_OpenSearch.py', 'create_OpenSearch_client')

    index_name='deals'
    directory = '/opt/airflow/deals_data/files_from_api/deals_ready_to_load_to_open_search/'
    #directory=r'C:\Naya_Course\airflow\deals_data\files_from_api\deals_ready_to_load_to_open_search\\'
    with open(f'{directory}all_deals.json','r', encoding='utf-8') as f:
        all_deals_json=json.loads(f.read())
    delete_all_doc(client,index_name)
    insert_bulk_documents(all_deals_json, index_name, client)
    write_to_log('Info', f'ETL has finished successfully', 'insert_docs_to_OpenSearch.py', 'insert_documents_to_OpenSearch')
    write_to_log('----------', f'----------', '----------', '----------')

def main(all_deals_json={}):
    client=create_OpenSearch_client()
    print(f'client:{client}')    
    # search_word(client,index_name,word='מזוודות')
    # delete_deal(client,index_name,deal_id='htzone|PRO² shop סט 3 מזוודות טרולי קשיחות | משלוח חינם')

if __name__ == "__main__":
    pass
    main()