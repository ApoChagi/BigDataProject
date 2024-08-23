import json
import csv
from bs4 import BeautifulSoup

# Descriptions is in HTML format. Remove tags and unnecessary characters
def clean_string(input_string):
    try:
        input_string = input_string.replace('\n', '')
        soup = BeautifulSoup(input_string, 'html.parser')
        clean_string = soup.get_text(separator=' ')

        # Remove commas, semicolons, and nested double quotes
        clean_string = clean_string.replace(',', '').replace(';', '').replace('"', '')

        return clean_string

    except Exception as e:
        print(f"Error: {e}")
        return ""



def convert_json_to_csv(json_filename, csv_filename, total_books):

    all_cnt = 0

    # Open the CSV file for writing
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        
        # Create a CSV writer
        csv_writer = csv.writer(csv_file)

        # Write the header row
        header = [  'id',                   \
                    'title',                \
                    'author_id',            \
                    'author_name',          \
                    'language',             \
                    'average_rating',       \
                    'text_reviews_count',   \
                    'ratings_count',        \
                    'publication_year',     \
                    'publisher',            \
                    'num_pages',            \
                    'series_id',            \
                    'series_name',          \
                    'description'           \
                ]
        csv_writer.writerow(header)

        

        # Read the JSON file line by line
        with open(json_filename, 'r', encoding='utf-8') as json_file:
            for line in json_file:
                try:
                    # Attempt to load each line as a JSON object
                    book = json.loads(line)
                    all_cnt += 1

                    pub_date = book.get('publication_date', '')
                    year = pub_date.split('-')[0] if pub_date else ''
                    description = book.get('description', '')
                    description = clean_string(description)
                    
                    #print(description)
                    if all_cnt > total_books:
                        return 
                    
                    # Write data for each book
                    csv_writer.writerow([
                        book.get('id', ''),
                        book.get('title', '').replace(';', '').replace('\n', ''),
                        book.get('author_id', ''),
                        book.get('author_name', ''),
                        book.get('language', ''),
                        book.get('average_rating', ''),       
                        book.get('text_reviews_count', ''),
                        book.get('ratings_count', ''),
                        year,       
                        book.get('publisher', '').replace(';', '').replace('\n', ''),
                        book.get('num_pages', ''),
                        book.get('series_id', ''),
                        book.get('series_name', '').replace(';', '').replace('\n', ''),
                        description
                ])
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

# Replace 'your_input_file.json' and 'your_output_file.csv' with your actual filenames
convert_json_to_csv('books.json', 'books_smaller.csv', 23000000)

