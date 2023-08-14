# Simple MySQL Database Migration Script

This script facilitates the migration of data and schema from a source MySQL database to a destination MySQL database. It uses the MySQL Connector library to establish connections, retrieve DDL statements, and migrate data while handling duplicates.

## Features

- Retrieves CREATE TABLE and CREATE VIEW statements from the source database.
- Handles duplicates during data migration using the `REPLACE INTO` statement.
- Provides a class-based structure for easy configuration and execution.

## Prerequisites

- Python 3.x
- Required Python libraries: `mysql-connector-python`, `tqdm`

Install the required libraries using:

```bash
pip install mysql-connector-python tqdm


## Usage

Clone or download this repository to your local machine.
Open the script in a text editor.
Modify the source_conn and dest_conn dictionaries to match your source and destination database configurations.
Run the script using the following command:

```bash
python your_script_name.py

## Configuration

You can adjust various settings within the script, such as batch size for data migration and error handling for table creation.

## Contributing

Contributions to enhance and extend this script are welcome! Feel free to fork the repository, make changes, and submit pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.


## Note: 
##### This script is intended for educational and reference purposes. Always ensure you have proper backups and perform testing before migrating critical data.


## execution using docker
#### Build the Docker Image:
docker build -t SimpleMigrateMySqlDB

#### Run the Docker Container
For questions or support, please contact kavingwa@icloud.com
docker run -it --rm SimpleMigrateMySqlDB.py
