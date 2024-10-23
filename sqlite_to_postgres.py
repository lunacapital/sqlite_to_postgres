import re
import os

def convert_sqlite_to_postgres(input_file, output_file):
    # Dictionaries to store table columns and track which are BOOLEAN
    table_columns = {}
    bool_columns = set()

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            line = line.strip()

            # Remove PRAGMA statements
            if line.startswith('PRAGMA'):
                continue

            # Replace BEGIN TRANSACTION and COMMIT TRANSACTION
            if line == 'BEGIN TRANSACTION;':
                outfile.write('BEGIN;\n')
                continue
            if line == 'COMMIT;':
                outfile.write('COMMIT;\n')
                continue

            # Skip internal SQLite tables
            if 'sqlite_sequence' in line:
                continue

            # Handle CREATE INDEX statements
            if line.startswith('CREATE INDEX') or line.startswith('CREATE UNIQUE INDEX'):
                line = line.replace('"', '')  # Remove unnecessary quotes
                outfile.write(line + '\n')
                continue

            # Handle CREATE TABLE statements
            if line.startswith('CREATE TABLE'):
                # Capture the entire CREATE TABLE statement
                create_table_sql = [line]
                while not line.endswith(');'):
                    line = infile.readline().strip()
                    create_table_sql.append(line)
                create_table_statement = ' '.join(create_table_sql)

                # Extract table name and columns
                table_match = re.match(r'CREATE TABLE IF NOT EXISTS "?([\w]+)"? \((.*)\);', create_table_statement, re.DOTALL)
                if table_match:
                    current_table = table_match.group(1)
                    columns_section = table_match.group(2)

                    # Split columns by comma not within parentheses
                    columns_raw = re.split(r',\s*(?![^()]*\))', columns_section)

                    columns = []
                    modified_columns = []

                    for column_def in columns_raw:
                        column_def = column_def.strip()

                        # Convert AUTOINCREMENT to SERIAL
                        if 'AUTOINCREMENT' in column_def.upper():
                            column_def = re.sub(r'INTEGER\s+.*AUTOINCREMENT', 'SERIAL', column_def, flags=re.IGNORECASE)
                            column_def = re.sub(r'PRIMARY KEY', '', column_def, flags=re.IGNORECASE)
                            column_def = column_def.strip()
                            if not column_def.endswith('PRIMARY KEY'):
                                column_def += ' PRIMARY KEY'

                        # Replace BOOL with BOOLEAN
                        column_def = re.sub(r'\bBOOL\b', 'BOOLEAN', column_def, flags=re.IGNORECASE)

                        # Replace DATETIME with TIMESTAMP
                        column_def = re.sub(r'\bDATETIME\b', 'TIMESTAMP', column_def, flags=re.IGNORECASE)

                        # Extract column names
                        column_name_match = re.match(r'"?([\w]+)"?', column_def)
                        if column_name_match:
                            column_name = column_name_match.group(1)
                            columns.append(column_name)

                            # Check for BOOLEAN columns
                            if 'BOOLEAN' in column_def.upper():
                                bool_columns.add((current_table, column_name))

                        modified_columns.append(column_def)

                    # Store columns for INSERT statements
                    table_columns[current_table] = columns

                    # Reconstruct CREATE TABLE statement
                    modified_columns_str = ',\n    '.join(modified_columns)
                    create_table_stmt = f'CREATE TABLE IF NOT EXISTS "{current_table}" (\n    {modified_columns_str}\n);\n'
                    outfile.write(create_table_stmt)
                else:
                    # If the regex doesn't match, write the original line
                    outfile.write(create_table_statement + '\n')
                continue

            # Handle INSERT INTO statements
            if line.startswith('INSERT INTO'):
                insert_match = re.match(r'INSERT INTO "?([\w]+)"? VALUES\((.*)\);', line)
                if insert_match:
                    table_name = insert_match.group(1)
                    values_section = insert_match.group(2)

                    # Split values while considering commas inside quotes
                    values = re.findall(r"(?:'[^']*'|[^,])+?", values_section)

                    # Process only if table structure is known
                    if table_name in table_columns:
                        columns = table_columns[table_name]

                        # Check for column-value count mismatch
                        if len(columns) != len(values):
                            print(f"Warning: Mismatch in columns and values for table '{table_name}'. Skipping this INSERT.")
                            continue

                        # Process BOOLEAN columns
                        for i, value in enumerate(values):
                            column_name = columns[i]
                            if (table_name, column_name) in bool_columns:
                                val = value.strip().strip("'")
                                if val == '0':
                                    values[i] = 'FALSE'
                                elif val == '1':
                                    values[i] = 'TRUE'

                        columns_str = ', '.join(f'"{col}"' for col in columns)
                        values_str = ', '.join(values)
                        insert_stmt = f'INSERT INTO "{table_name}" ({columns_str}) VALUES({values_str});\n'
                        outfile.write(insert_stmt)
                    else:
                        # Skip if table not defined
                        print(f"Warning: Skipping INSERT INTO for table '{table_name}' as CREATE TABLE is not found.")
                continue

            # Write any other lines as-is
            outfile.write(line + '\n')

    print(f"Conversion complete! The PostgreSQL-compatible SQL is saved to {output_file}.")

if __name__ == '__main__':
    current_dir = os.getcwd()
    input_file = os.path.join(current_dir, 'sqlite_dump.sql')
    output_file = os.path.join(current_dir, 'postgres_dump.sql')

    convert_sqlite_to_postgres(input_file, output_file)
