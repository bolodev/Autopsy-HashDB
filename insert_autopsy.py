import argparse
import os
import sqlite3
import sys

def main():
    hash_count = 0
    hash_connection = None
    if not os.path.isfile(args.output):
        print("[INFO] Creating database: {}".format(args.output))
        hash_connection = sqlite3.connect(args.output)
        hash_connection.execute("CREATE TABLE db_properties (name TEXT NOT NULL, value TEXT)")
        hash_connection.execute("CREATE TABLE hashes (id INTEGER PRIMARY KEY AUTOINCREMENT, md5 BINARY(16) UNIQUE, sha1 BINARY(20), sha2_256 BINARY(32))")
        hash_connection.execute("CREATE TABLE file_names (name TEXT NOT NULL, hash_id INTEGER NOT NULL, PRIMARY KEY(name, hash_id))")
        hash_connection.execute("CREATE TABLE comments (comment TEXT NOT NULL, hash_id INTEGER NOT NULL, PRIMARY KEY(comment, hash_id))")
        print("[INFO] Database Created.")
    else:
        print("[INFO] Connecting to database: {}".format(args.output))
        hash_connection = sqlite3.connect(args.output)
        print("Dropping index md5_index.")
        try:
            hash_connection.execute("DROP INDEX md5_index")
        except sqlite3.OperationalError as e:
            pass

    print("[INFO] Opening hash file {}.".format(args.input))
    with open(args.input, "r") as hash_file:
        for file_line in hash_file.readlines():
            hash_count += 1
            try:
                hash_line = file_line.rstrip()
                if not hash_line.startswith("#"):
                    if len(hash_line) == 32:
                        hash_connection.execute("INSERT INTO hashes (md5) VALUES (x'{}')".format(hash_line))
                    else:
                        print("[WARNING] Line: {1} not the right length for an MD5 hash, length is {2}.".format(hash_count, len(hash_line)))
            except (IndexError, sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                print("{}".format(e.args))
                continue
            #print("[DEBUG] Count: {}".format(hash_count))
    hash_file.close()
 
    print("[INFO] Finished inserts, committing changes.")
    hash_connection.commit()
    if not args.noindex:
        print("[INFO] Creating index on hashes.md5")
        hash_connection.execute("CREATE INDEX md5_index ON hashes(md5)")
    hash_connection.close()
    print("[INFO] Finished reading {} hashes.".format(hash_count))
    
def print_usage():
    """
    Print the command line usage
    """
    print("\nUSAGE:\n")
    print("python3 insert_autopsy.py -i input_file -o some_database_to_write_to\n")
    print("For help:")
    print("python3 insert_autopsy.py -h or python3 insert_autopsy.py --help")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input file of hashes")
    parser.add_argument("-o", "--output", help="output or existing database")
    parser.add_argument("-n", "--noindex", action="store_true", help="do not create index, use if adding another file")
    args = parser.parse_args()
    if not args.input and not args.output:
        print("[ERROR] no input or output options set.")
        print_usage()
    elif args.input and not args.output:
        print("[ERROR] no output option set.")
        print_usage()
    elif args.output and not args.input:
        print("[ERROR] no input options set.")
        print_usage()
    else:
        if not os.path.isfile(args.input):
            print("[ERROR] Input hash file does not exist. Ensure the input hash file exists and is accessible.")
            sys.exit(1)
        if not args.output:
            print("[ERROR] Output database not defined.")
            sys.exit(1)
        main()
