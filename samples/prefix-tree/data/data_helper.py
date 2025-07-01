#!/usr/bin/env python3

import csv
import sys
import os

def process_csv(input_file_path):
    """
    Process the input CSV file to create inputs.csv and queries.csv
    
    Args:
        input_file_path (str): Path to the input CSV file
    """
    
    # Check if input file exists
    if not os.path.exists(input_file_path):
        print(f"Error: File '{input_file_path}' not found.")
        sys.exit(1)
    
    names = []
    queries = []
    
    try:
        with open(input_file_path, 'r', encoding='utf-8') as file:
            # Use csv.DictReader to handle the CSV with headers
            reader = csv.DictReader(file)
            
            for row in reader:
                # Extract the name
                name = row.get('Name', '').strip()
                if name:
                    names.append(name)
                
                # Extract synonyms and get first 3
                synonyms = row.get('Synonyms', '').strip()
                if synonyms:
                    # Split by pipe character and get first 3
                    synonym_list = [s.strip() for s in synonyms.split('|') if s.strip()]
                    first_three = synonym_list[:3]
                    queries.extend(first_three)
        
        # Write inputs.csv (names)
        with open('inputs.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Name'])  # Header
            for name in names:
                writer.writerow([name])
        
        # Write queries.csv (first 3 synonyms from each row)
        with open('queries.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Query'])  # Header
            for query in queries:
                writer.writerow([query])
        
        print(f"Successfully processed {len(names)} compounds.")
        print(f"Created 'inputs.csv' with {len(names)} names.")
        print(f"Created 'queries.csv' with {len(queries)} queries.")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)

def main():
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_csv_file>")
        print("Example: python script.py data.csv")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    process_csv(input_file_path)

if __name__ == "__main__":
    main()
