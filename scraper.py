import requests
import pandas as pd
import time

def fetch_and_stream_papers(topic, output_filename="openalex_streamed.csv", limit=10000):
    """
    Fetches papers from OpenAlex and streams them directly to a CSV file 200 items at a time
    to keep memory footprint near zero.
    """
    headers = {
        'User-Agent': 'shauryachauhan2050@gmail.com' # Use your real email
    }
    
    base_url = "https://api.openalex.org/works"
    
    params = {
        'search': topic,
        'filter': 'has_abstract:true',  # Server-side filtering
        'per_page': 200,
        'cursor': '*',
        'api_key': 'n66C3l0O086hALzQQDEDq4'
    }
    # Define the exact columns matching your desired schema
    columns = [
        'openalex_id', 'title', 'doi', 'abstract', 'publish_time', 
        'authors', 'journal', 'pmcid', 'pubmed_id'
    ]
    
    # STEP 1: Clear the existing file and write ONLY the header row
    pd.DataFrame(columns=columns).to_csv(output_filename, index=False)
    print(f"Initialized clean file: {output_filename}")

    total_collected = 0
    
    # STEP 2: Stream data using the API cursor loop
    while params['cursor'] and total_collected < limit:
        try:
            print(f"Progress: Fetching next batch... Total saved so far: {total_collected} / {limit}")
            
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            if not results:
                print("No more results available.")
                break
            
            batch_data = []
            
            for paper in results:
                # 1. Reconstruct the abstract
                abstract_index = paper.get('abstract_inverted_index', {})
                abstract = ""
                if abstract_index:
                    words = []
                    for word, positions in abstract_index.items():
                        for pos in positions:
                            words.append((pos, word))
                    words.sort()
                    abstract = ' '.join([word for _, word in words])

                if not abstract.strip():
                    continue

                # 2. Extract author names
                authors = [auth.get('author', {}).get('display_name', '') 
                           for auth in paper.get('authorships', [])]
                authors = [name for name in authors if name]

                # 3. Extract venue
                journal = ''
                primary_location = paper.get('primary_location')
                if primary_location and primary_location.get('source'):
                    journal = primary_location['source'].get('display_name', '')

                # 4. Append to temporary batch list
                batch_data.append({
                    'openalex_id': paper.get('id', ''),
                    'title': paper.get('title', ''),
                    'doi': paper.get('doi', ''),
                    'abstract': abstract,
                    'publish_time': paper.get('publication_date', ''),
                    'authors': '; '.join(authors),
                    'journal': journal,
                    'pmcid': paper.get('ids', {}).get('pmcid', ''),
                    'pubmed_id': paper.get('ids', {}).get('pmid', '')
                })
                
                total_collected += 1
                if total_collected >= limit:
                    break
            
            # STEP 3: Append the current batch to the CSV and wipe the temporary batch list
            if batch_data:
                batch_df = pd.DataFrame(batch_data, columns=columns)
                # mode='a' means append, header=False prevents re-writing column names
                batch_df.to_csv(output_filename, mode='a', header=False, index=False)
            
            # Update cursor for next batch
            params['cursor'] = data.get('meta', {}).get('next_cursor')
            time.sleep(0.1)
            
        except Exception as e:
            print(f"\nAn error occurred: {e}")
            break
            
    print(f"\nSuccess! Process finished. {total_collected} rows saved in total.")

# Execution
if __name__ == "__main__":
    CSV_NAME = "scraped.csv"
    
    fetch_and_stream_papers(
        topic="Space Biology", 
        output_filename=CSV_NAME, 
        limit=1000000
    ) 