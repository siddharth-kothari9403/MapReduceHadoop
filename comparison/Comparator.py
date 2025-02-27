import re

def preprocess_text_file(file_path):
    """Preprocess the raw document file: remove symbols, lowercase, and store words in order."""
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Remove all non-alphabetic characters (except spaces)
    text = re.sub(r'[^a-zA-Z\s]', '', text).lower().split()

    return text  # Return list of words in order

def load_output_file(file_path):
    """Load the output.txt file into an ordered list of (index, word)."""
    index_word_map = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                index = int(parts[0])
                word = parts[1].lower()  # Ensure lowercase
                index_word_map.append((index, word))
    return index_word_map

def compare_documents(correct_words, output_data):
    """Compare words while aligning missing indices in output.txt."""
    differences = []
    i = 0

    while i < len(output_data) and i < len(correct_words):
        index, output_word = output_data[i]

        # Align document index with output index
        correct_word = correct_words[i]

        # Compare words
        if correct_word != output_word:
            differences.append(f"Index {index}: '{correct_word}' ≠ '{output_word}'")

        # Move to next word
        i += 1
    
    while i < len(output_data):
        
        index, output_word = output_data[i]
        differences.append(f"Index {index}: {output_word} absent in original document")
        i+=1

    return differences

# Paths to files
raw_doc_path = "563203.txt"    # The original document file
output_doc_path = "output_q3_2.txt"   # The mapper-reducer output

# Process files
processed_words = preprocess_text_file(raw_doc_path)
# print(processed_words)
output_data = load_output_file(output_doc_path)
# print(output_data)

# Compare
differences = compare_documents(processed_words, output_data)

# Print differences
if differences:
    print("Differences found:")
    for diff in differences:
        print(diff)
else:
    print("Documents match exactly!")
