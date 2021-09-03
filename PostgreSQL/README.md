# PostgreSQL

A PostgreSQL testing playground organized by topic in the SQL tutorial here: https://www.tutorialspoint.com/postgresql/index.htm

## Installation

### Local machine

PostgreSQL 10.18 was installed for the purpose of interfacing with Metabase. The installers can be found here:

* [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)

You will also need to install addtional dependencies. To install, enter:

```
pip install psycopg2
```

## Usage

Once installed, the server needs to be run by navigating to the `bin` folder of the PostgreSQL installation, opening cmd and entering:

```
.\pg_ctl.exe start -D "C:\Program Files\PostgreSQL\10\data"
```

`start` - starts the server. Use `stop` to stop it.
`-D data_folder` - determines the path to where the database files are stored.

Other commands can be found here: https://www.postgresql.org/docs/10/app-pg-ctl.html
