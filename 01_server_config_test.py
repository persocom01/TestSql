# Creates the server.cfg file.
import json

server_config = {
    'user': 'root',
    # 'password': '1234',
    'host': '127.0.0.1',
    'port': 3306,
    # 'database': 'testDB',
    # Unknown how to set local_infile.
    # 'local_infile': True
}

cfg_path = './server.cfg'

# Create human readable json file.
with open(cfg_path, 'w') as f:
    json.dump(server_config, f, sort_keys=True, indent=4, ensure_ascii=False)
