# TestSQL

An SQL testing playground organized by topic in the SQL tutorial here: https://www.tutorialspoint.com/sql/index.htm

## Installation

### AWS RDS

### Local machine

This project used the database server from the WinNMP server stack. Python was used to connect to the database server and Atom was used as text editor.

* [WinNMP 20.01](https://winnmp.wtriple.com/)
* [python 3.8.1 Windows x86-64](https://www.python.org/downloads/)
* [atom 1.43.0](https://atom.io/)

pip was used to install mariadb python connectors. To install, open cmd and type:

```
pip install mariadb
pip install sqlalchemy
pip install pymysql
```

This installs following modules:

* mariadb - the official mariadb python connector.
* sqlalchemy - a comprehensive set of tools for working with databases and Python.
* pymysql - the api for sqlalchemy's engine.

### Atom packages used:

* Hydrogen
* linter-flake8
* python-autopep8

### General packages:

* atom-beautify
* busy-signal
* file-icons
* intentions
* minimap
* open_in_cmd
* project-manager
* script

## SQL injections

With SQL comes the danger of SQL injection attacks. These are pieces of SQL code inserted as values in SQL commands with variables. There are several ways to prevent SQL injection attacks:

1. Putting quotes on values when running SQL statements to turn them into strings

```
f'INSERT INTO table_name(col1, col2) VALUES ("{value1}", "{value2}");'
```
