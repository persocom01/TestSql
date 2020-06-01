# SQLite

SQLite is a relational database management system (RDBMS) that is not a client-server engine, but is instead embedded into applications.

## Installation

### SQLite on AWS

1. Spin up an Ubuntu 18.04 AWS EC2 and configure it to accept SSH and TCP 5601 (kibana) connections from anywhere.

2. Download the .pem key.

3. git bash (install on computer if not already present) in the folder with the key and type:

```
chmod 400 keyname.pem
```

which gives the user permission to read the file (4) and no permissions (0) to the group and everyone else.

4. Connect to the aws instance using the following command:

```
ssh -i keyname.pem ubuntu@aws_instance_public_dns
```

To remove the added ip from the known hosts list, use:

```
ssh-keygen -R server_ip_address
```

To end the connection, enter:

```
exit
```

5. Install SQLite.

Before ending the connection, we need to actually install elasticsearch on the machine. Do so by running the following commands:

```
sudo apt-get update
sudo apt-get install sqlite3
```

This project used the database server from the WinNMP server stack. Python was used to connect to the database server and Atom was used as text editor.

* [WinNMP 20.01](https://winnmp.wtriple.com/)
* [python 3.8.1 Windows x86-64](https://www.python.org/downloads/)
* [atom 1.43.0](https://atom.io/)

pip was used to install mariadb python connectors. To install, open cmd and type:

```
pip install --pre mariadb
pip install sqlalchemy
pip install pymysql
```

This installs following modules:

* mariadb - the offical mariadb python connector.
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
