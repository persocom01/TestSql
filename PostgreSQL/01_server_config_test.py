# Creates the server.cfg file.
import json

# Aside from this, if connecting to a cloud server over the net, ensure that it
# is set to publicly available.
server_config = {
    # This is the PostgreSQL superuser.
    'user': 'postgres',
    'password': 'admin',
    'host': '127.0.0.1',
    'port': 5432,
    'database': 'food_order2'
}

cfg_path = './PostgreSQL/server.cfg'

# Create human readable json file.
with open(cfg_path, 'w') as f:
    json.dump(server_config, f, sort_keys=True, indent=4, ensure_ascii=False)
