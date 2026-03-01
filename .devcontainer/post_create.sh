#!/bin/sh

uvx --from git+https://github.com/oraios/serena serena start-mcp-server --help
npm install -g @modelcontextprotocol/server-filesystem
curl -fsSL https://claude.ai/install.sh | bash
sudo apt update && sudo apt install -y ripgrep jq